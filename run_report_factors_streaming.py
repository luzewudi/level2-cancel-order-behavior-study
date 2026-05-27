from __future__ import annotations

"""
按交易日流式执行撤单因子复现：
1. 为单个交易日生成精简 Parquet 缓存；
2. 用缓存计算并写入 raw 因子矩阵；
3. 写入完成 tag；
4. 删除该交易日缓存；
5. 下次启动只处理 tag 后面的日期。
"""

import argparse
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from numpy.lib.format import open_memmap
from tqdm.auto import tqdm

import config
from build_level2_parquet_cache import ConvertResult, build_tasks_for_date, convert_one
from build_report_factors import (
    LEVEL2_PARQUET_CACHE_DIR,
    build_roll_outputs,
    factor_names,
    load_axes,
    normalize_stock_code,
    parse_csv_arg,
    process_date,
    scan_dates,
    validate_axes,
)
from utils.log_kit import get_logger


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = get_logger("run_report_factors_streaming")

TAG_PATH = config.REPORT_FACTOR_OUTPUT_DIR / "streaming_last_completed_date.tag"


def read_tag(tag_path: Path) -> str | None:
    if not tag_path.exists():
        return None
    text = tag_path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    if not (text.isdigit() and len(text) == 8):
        raise ValueError(f"tag 文件内容不是 YYYYMMDD：{tag_path} -> {text!r}")
    return text


def write_tag(tag_path: Path, date: str) -> None:
    tag_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = tag_path.with_suffix(tag_path.suffix + ".tmp")
    tmp_path.write_text(date + "\n", encoding="utf-8")
    tmp_path.replace(tag_path)


def open_or_create_raw_memmaps(
    names: list[str],
    shape: tuple[int, int],
    output_dir: Path,
) -> dict[str, np.memmap]:
    """打开已有 raw 矩阵；缺失的矩阵创建并填 NaN。"""

    output_dir.mkdir(parents=True, exist_ok=True)
    arrays: dict[str, np.memmap] = {}
    for name in names:
        path = output_dir / f"{name}.npy"
        if path.exists():
            arr = np.load(path, mmap_mode="r+")
            if arr.shape != shape:
                raise ValueError(f"{path} 形状 {arr.shape} 与 EOD 轴形状 {shape} 不一致")
            arrays[name] = arr
            continue

        arr = open_memmap(path, mode="w+", dtype=np.float64, shape=shape)
        for start in range(0, shape[0], 128):
            arr[start : start + 128, :] = np.nan
        arr.flush()
        arrays[name] = arr
    return arrays


def flush_arrays(arrays: dict[str, np.memmap]) -> None:
    for arr in arrays.values():
        arr.flush()


def build_cache_for_date(date: str, stock_filter: set[str] | None, n_jobs: int) -> None:
    tasks = build_tasks_for_date(date, stock_filter, overwrite=True)
    if not tasks:
        raise ValueError(f"{date}: 没有找到需要转换的 order/trade 文件")

    workers = max(1, min(int(n_jobs), len(tasks)))
    logger.info(f"{date}: 开始生成临时 Parquet 缓存，files={len(tasks)}, workers={workers}")

    failures: list[ConvertResult] = []
    converted = 0
    rows = 0
    if workers == 1:
        iterator = (convert_one(task) for task in tasks)
        for result in tqdm(iterator, total=len(tasks), desc=f"{date} 缓存转换", dynamic_ncols=True):
            if result.status == "failed":
                failures.append(result)
            else:
                converted += int(result.status == "converted")
                rows += result.rows
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(convert_one, task): task for task in tasks}
            for future in tqdm(
                as_completed(future_map),
                total=len(future_map),
                desc=f"{date} 缓存转换",
                dynamic_ncols=True,
            ):
                result = future.result()
                if result.status == "failed":
                    failures.append(result)
                else:
                    converted += int(result.status == "converted")
                    rows += result.rows

    logger.info(f"{date}: 临时缓存完成，converted={converted}, failed={len(failures)}, rows={rows}")
    for failure in failures[:20]:
        logger.error(f"{date} {failure.table} {failure.stock_code}: {failure.message}")
    if failures:
        raise RuntimeError(f"{date}: 存在 {len(failures)} 个文件转换失败")


def delete_cache_for_date(date: str) -> None:
    cache_dir = (LEVEL2_PARQUET_CACHE_DIR / date).resolve()
    cache_root = LEVEL2_PARQUET_CACHE_DIR.resolve()
    if cache_dir == cache_root or cache_root not in cache_dir.parents:
        raise ValueError(f"拒绝删除非缓存日期目录：{cache_dir}")
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.info(f"{date}: 已删除临时缓存 {cache_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="逐日生成缓存、计算因子、打 tag、删除缓存。")
    parser.add_argument("--dates", help="可选：逗号分隔交易日；仍会跳过 tag 及以前的日期。")
    parser.add_argument("--stocks", help="可选：逗号分隔股票代码，主要用于小样本测试。")
    parser.add_argument("--n-jobs", type=int, default=config.N_JOBS)
    parser.add_argument("--tag-path", type=Path, default=TAG_PATH)
    parser.add_argument("--skip-roll", action="store_true", help="只更新 raw，不在最后生成 roll5/roll20。")
    parser.add_argument("--force-from", help="忽略已有 tag，从指定 YYYYMMDD 开始处理。")
    parser.add_argument("--dry-run", action="store_true", help="只打印将处理的日期，不生成缓存或因子。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    requested_dates = parse_csv_arg(args.dates)
    requested_stocks = parse_csv_arg(args.stocks)
    if requested_stocks is not None:
        requested_stocks = {normalize_stock_code(stock) for stock in requested_stocks}

    tickers, dates_axis, ticker_to_idx, date_to_idx = load_axes()
    validate_axes(tickers, dates_axis)
    shape = (len(tickers), len(dates_axis))
    names = factor_names()

    last_completed = read_tag(args.tag_path)
    if args.force_from:
        force_from = args.force_from.strip()
        if not (force_from.isdigit() and len(force_from) == 8):
            raise ValueError("--force-from 必须是 YYYYMMDD")
        last_completed = None
    else:
        force_from = None

    process_dates = [date for date in scan_dates(requested_dates) if date in date_to_idx]
    if force_from is not None:
        process_dates = [date for date in process_dates if date >= force_from]
    elif last_completed is not None:
        process_dates = [date for date in process_dates if date > last_completed]

    if not process_dates:
        logger.info(f"没有需要处理的新日期，tag={last_completed}, tag_path={args.tag_path}")
        return

    logger.info(
        f"启动流式撤单因子任务：dates={len(process_dates)}, first={process_dates[0]}, "
        f"last={process_dates[-1]}, tag={last_completed}, cache_dir={LEVEL2_PARQUET_CACHE_DIR}"
    )
    if args.dry_run:
        logger.info(f"dry-run 日期列表：{process_dates}")
        return

    raw_dir = config.REPORT_FACTOR_OUTPUT_DIR / "raw"
    arrays = open_or_create_raw_memmaps(names, shape, raw_dir)
    tradable = np.load(config.TRADABLE_NPY_PATH, mmap_mode="r")
    free_float = np.load(config.FREE_FLOAT_SHARES_NPY_PATH, mmap_mode="r")

    try:
        for date in process_dates:
            date_idx = date_to_idx[date]
            logger.info(f"{date}: 开始流式处理")
            build_cache_for_date(date, requested_stocks, args.n_jobs)
            process_date(
                date=date,
                date_idx=date_idx,
                ticker_to_idx=ticker_to_idx,
                tradable_matrix=tradable,
                free_float=free_float,
                arrays=arrays,
                stock_filter=requested_stocks,
                n_jobs=args.n_jobs,
                dry_run=False,
            )
            flush_arrays(arrays)
            write_tag(args.tag_path, date)
            logger.info(f"{date}: 已写入 tag {args.tag_path}")
            delete_cache_for_date(date)
            logger.info(f"{date}: 完成流式处理")
    finally:
        flush_arrays(arrays)
        del arrays

    if not args.skip_roll:
        build_roll_outputs(names, raw_dir, config.REPORT_FACTOR_OUTPUT_DIR, shape)

    logger.info("流式撤单因子任务完成")


if __name__ == "__main__":
    main()
