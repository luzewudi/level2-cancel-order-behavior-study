from __future__ import annotations

"""
把 Level-2 原始 csv/csv.gz 转成只含因子计算所需列的 Parquet 缓存。

生成目录：
  cache/level2_parquet / YYYYMMDD / order/*.parquet
  cache/level2_parquet / YYYYMMDD / trade/*.parquet
"""

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from tqdm.auto import tqdm

import config
from build_report_factors import (
    LEVEL2_PARQUET_CACHE_DIR,
    ORDER_READ_COLUMNS,
    TRADE_READ_COLUMNS,
    build_file_map,
    normalize_stock_code,
    parse_csv_arg,
    read_csv_auto,
    scan_dates,
)
from utils.log_kit import get_logger


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = get_logger("build_level2_parquet_cache")


@dataclass(frozen=True)
class ConvertTask:
    table: str
    date: str
    stock_code: str
    source_path: str
    target_path: str
    columns: tuple[str, ...]
    overwrite: bool


@dataclass(frozen=True)
class ConvertResult:
    table: str
    date: str
    stock_code: str
    status: str
    rows: int
    source_path: str
    target_path: str
    message: str = ""


def cache_stem(path: Path) -> str:
    """保留原始文件名中的交易所信息，只替换扩展名。"""

    name = path.name
    if name.endswith(".csv.gz"):
        return name[:-7]
    if name.endswith(".csv"):
        return name[:-4]
    return path.stem


def convert_one(task: ConvertTask) -> ConvertResult:
    source_path = Path(task.source_path)
    target_path = Path(task.target_path)
    if target_path.exists() and not task.overwrite:
        return ConvertResult(
            table=task.table,
            date=task.date,
            stock_code=task.stock_code,
            status="skipped",
            rows=0,
            source_path=task.source_path,
            target_path=task.target_path,
        )

    try:
        df = read_csv_auto(source_path, columns=list(task.columns))
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
        df.write_parquet(tmp_path, compression="zstd")
        tmp_path.replace(target_path)
        return ConvertResult(
            table=task.table,
            date=task.date,
            stock_code=task.stock_code,
            status="converted",
            rows=df.height,
            source_path=task.source_path,
            target_path=task.target_path,
        )
    except Exception as exc:  # noqa: PERF203
        return ConvertResult(
            table=task.table,
            date=task.date,
            stock_code=task.stock_code,
            status="failed",
            rows=0,
            source_path=task.source_path,
            target_path=task.target_path,
            message=str(exc),
        )


def build_tasks_for_date(date: str, stock_filter: set[str] | None, overwrite: bool) -> list[ConvertTask]:
    tasks: list[ConvertTask] = []
    cache_root = LEVEL2_PARQUET_CACHE_DIR / date

    for table, columns in (("order", ORDER_READ_COLUMNS), ("trade", TRADE_READ_COLUMNS)):
        raw_map = build_file_map(config.LEVEL2_DATA_DIR / date / table)
        for stock_code, source_path in sorted(raw_map.items()):
            if stock_filter is not None and stock_code not in stock_filter:
                continue
            target_path = cache_root / table / f"{cache_stem(source_path)}.parquet"
            tasks.append(
                ConvertTask(
                    table=table,
                    date=date,
                    stock_code=stock_code,
                    source_path=str(source_path),
                    target_path=str(target_path),
                    columns=tuple(columns),
                    overwrite=overwrite,
                )
            )
    return tasks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="构建 Level-2 精简 Parquet 缓存。")
    parser.add_argument("--dates", help="逗号分隔的交易日，例如 20240226,20200730。")
    parser.add_argument("--stocks", help="逗号分隔的股票代码，例如 000001,600000。")
    parser.add_argument("--n-jobs", type=int, default=config.N_JOBS)
    parser.add_argument("--overwrite", action="store_true", help="覆盖已存在的 Parquet 缓存。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    requested_dates = parse_csv_arg(args.dates)
    requested_stocks = parse_csv_arg(args.stocks)
    if requested_stocks is not None:
        requested_stocks = {normalize_stock_code(stock) for stock in requested_stocks}

    dates = scan_dates(requested_dates)
    if not dates:
        raise ValueError("没有找到满足筛选条件的 Level-2 交易日")

    all_tasks: list[ConvertTask] = []
    for date in dates:
        all_tasks.extend(build_tasks_for_date(date, requested_stocks, args.overwrite))
    if not all_tasks:
        raise ValueError("没有找到需要转换的 order/trade 文件")

    workers = max(1, min(int(args.n_jobs), len(all_tasks)))
    logger.info(
        f"启动 Parquet 缓存构建：dates={len(dates)}, files={len(all_tasks)}, "
        f"workers={workers}, cache_dir={LEVEL2_PARQUET_CACHE_DIR}"
    )

    counts = {"converted": 0, "skipped": 0, "failed": 0}
    rows = 0
    failures: list[ConvertResult] = []

    if workers == 1:
        iterator = (convert_one(task) for task in all_tasks)
        for result in tqdm(iterator, total=len(all_tasks), desc="转换 Parquet", dynamic_ncols=True):
            counts[result.status] += 1
            rows += result.rows
            if result.status == "failed":
                failures.append(result)
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(convert_one, task): task for task in all_tasks}
            for future in tqdm(
                as_completed(future_map),
                total=len(future_map),
                desc="转换 Parquet",
                dynamic_ncols=True,
            ):
                result = future.result()
                counts[result.status] += 1
                rows += result.rows
                if result.status == "failed":
                    failures.append(result)

    logger.info(
        f"Parquet 缓存构建完成：converted={counts['converted']}, "
        f"skipped={counts['skipped']}, failed={counts['failed']}, rows={rows}"
    )
    for failure in failures[:20]:
        logger.error(f"{failure.date} {failure.table} {failure.stock_code}: {failure.message}")
    if failures:
        raise RuntimeError(f"存在 {len(failures)} 个文件转换失败，详见日志")


if __name__ == "__main__":
    main()
