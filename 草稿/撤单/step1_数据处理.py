from __future__ import annotations

"""
step1_数据处理.py

统一脚本：直接基于原始 Level-2 三张表（Tick / Order / Trade）完成以下处理：
- 提取逐笔“撤单事件表”（不落盘，按日期与股票在内存中构造）
- 进行事件级别筛选与字段整理（包含对应原始委托时间 order_time_dt）
- 在撤单事件上进行分钟级聚合，识别“算法撤单”，输出分钟级因子

最终输出（按交易日，日频分钟级因子）：
- 因子缓存/日频分钟级/{date}.parquet
  主要字段：
    stock_code, date, minute,
    cancel_buy_count,  cancel_sell_count,
    cancel_buy_volume, cancel_sell_volume,
    algo_cancel_buy_count,  algo_cancel_sell_count,
    algo_cancel_buy_volume, algo_cancel_sell_volume

说明：
- 算法撤单识别规则：
  1）若存在 order_time_dt，则按 Δ = cancel_time_dt - order_time_dt（毫秒）
      在配置的时间窗口内（以 3 秒为步长的脉冲 + 半宽）识别算法撤单；
  2）否则，回退到相对连续竞价起点（09:30 / 13:00）的时间差 Δ 做近似识别。
"""

import gzip
import os
from pathlib import Path
import sys
from typing import Any, Iterable, List
import time

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
import warnings

# 多进程场景下强制数值库/Polars 单线程，避免“多进程 + 多线程”过度竞争导致卡顿/吞吐大幅下降
# 必须在 import polars / numpy 之前设置（尤其是在 Linux fork 场景下更稳妥）

# Polars 相关
os.environ.setdefault("POLARS_MAX_THREADS", "1")
os.environ.setdefault("POLARS_FORCE_SINGLE_THREAD", "1")

# NumPy / OpenBLAS / MKL 相关 - 强制单线程
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")

# 其他数值库
os.environ.setdefault("BLIS_NUM_THREADS", "1")
os.environ.setdefault("GOTO_NUM_THREADS", "1")

# 禁用多线程线程池
os.environ.setdefault("OMP_THREAD_LIMIT", "1")

import numpy as np
import polars as pl
import pandas as pd
from tqdm.auto import tqdm

import config
from utils.log_kit import get_logger

# 过滤 Polars 的 UserWarning（如关于 None 比较的警告）
warnings.filterwarnings("ignore", category=UserWarning, module="polars")
# 过滤全局 DeprecationWarning（包括 Polars 内部的一些弃用提示）
warnings.filterwarnings("ignore", category=DeprecationWarning)

logger = get_logger()
# 路径配置
BASE_DIR = config.BASE_DIR
LEVEL2_DATA_DIR = config.LEVEL2_DATA_DIR
# 因子缓存根目录；具体的“日频分钟级 / 全量”等子目录在后面保存时单独创建
CANCEL_MINUTE_DIR = BASE_DIR / "因子缓存"
# 单股票分钟级临时结果目录（按日期/股票拆分），用于方案一：子进程直接写出单股分钟因子
PER_STOCK_MINUTE_DIR = BASE_DIR / "cancel_minute_stock_tmp"

# 可交易股票配置（直接使用 numpy 文件，而不再使用 TradeStatus_labeled.csv）
TRADABLE_NPY_PATH = config.TRADABLE_NPY_PATH
TICKER_NAMES_NPY_PATH = config.TICKER_NAMES_NPY_PATH
DATES_NPY_PATH = config.DATES_NPY_PATH

# 懒加载缓存，避免每个交易日重复读取
_TRADABLE_MATRIX: np.ndarray | None = None
_TICKER_NAMES: list[str] | None = None
_DATES_8STR: list[str] | None = None
_DATE_INDEX_MAP: dict[str, int] | None = None

def _to_text(x: Any) -> str:
    """
    将 bytes / numpy.bytes_ / 其它类型稳健地转换为 str。
    - 兼容你提到的 ticker_names 里出现的 b'000024'（bytes）
    """
    try:
        if isinstance(x, (bytes, bytearray, np.bytes_)):  # type: ignore[attr-defined]
            b = bytes(x)
            for enc in ("utf-8", "gb18030"):
                try:
                    return b.decode(enc)
                except Exception:
                    pass
            return b.decode("utf-8", errors="replace")
    except Exception:
        # np.bytes_ 兼容性问题时回退
        pass
    return str(x)


def normalize_stock_code(code: Any) -> str:
    """将股票代码标准化为6位字符串（与草稿脚本一致）"""
    code_str = _to_text(code).strip()
    if not code_str:
        return code_str

    # 兼容多种形态：
    # - 000001 / 600000
    # - 000001.SZ / 000001-SZ / 000001_SZSE / 000001SZSE / SZ000001 / sh600000 等
    # 核心策略：提取其中的数字部分，优先取“最后 6 位”（避免诸如 20240226_000001 这类前缀污染）
    digits = "".join(ch for ch in code_str if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    if digits:
        return digits.zfill(6)

    # 无任何数字：原样返回（上游数据异常时便于排查）
    return code_str


def _build_local_dt_from_time_like(date: str, time_expr: pl.Expr) -> pl.Expr:
    """
    将数值/字符串形式的时间列统一转换为本地 Datetime：
    - 若值类似 UTC 纳秒时间戳（绝对值 >= 1e12），按 epoch ns +8 小时解析；
    - 否则按 HHMMSSmmm（如 92317250 表示 09:23:17.250）解释，并结合交易日 YYYYMMDD。
    """
    raw = time_expr.cast(pl.Int64, strict=False)

    # 分支1：UTC 纳秒时间戳（老数据格式）
    dt_epoch = pl.from_epoch(raw, time_unit="ns") + pl.duration(hours=8)

    # 分支2：HHMMSSmmm 整数编码，例如 92317250 -> "09:23:17.250"
    s = raw.abs().cast(pl.Utf8).str.zfill(9)
    hh = s.str.slice(0, 2)
    mm = s.str.slice(2, 2)
    ss = s.str.slice(4, 2)
    ms = s.str.slice(6, 3)
    dt_hms = (
        pl.concat_str(
            [
                pl.lit(date),
                pl.lit(" "),
                hh,
                pl.lit(":"),
                mm,
                pl.lit(":"),
                ss,
                pl.lit("."),
                ms,
            ]
        ).str.strptime(pl.Datetime, format="%Y%m%d %H:%M:%S.%3f", strict=False)
    )

    # 根据数值量级自动选择解析方式；若为 null 则整体结果也为 null
    return pl.when(raw.abs() >= 1_000_000_000_000).then(dt_epoch).otherwise(dt_hms)


def read_csv_auto(path: Path, schema_overrides: dict[str, Any] | None = None) -> pl.DataFrame:
    """
    读取 csv / csv.gz，兼容 UTF-8 与 gb18030。
    """
    encodings = [("utf-8", "strict"), ("gb18030", "replace")]
    last_err: Exception | None = None

    for enc, err_mode in encodings:
        fh = None
        try:
            if path.suffix == ".gz":
                fh = gzip.open(path, mode="rt", encoding=enc, errors=err_mode)
                df = pl.read_csv(fh, schema_overrides=schema_overrides)
            else:
                fh = path.open(mode="r", encoding=enc, errors=err_mode)
                df = pl.read_csv(fh, schema_overrides=schema_overrides)

            fh.close()
            fh = None
            return df
        except Exception as e:  # noqa: PERF203
            last_err = e
            logger.warning(
                f"[WARN] read_csv_auto failed file={path}, encoding={enc}, errors={err_mode}, err={e}"
            )
            if fh:
                try:
                    fh.close()
                except Exception:
                    pass
            continue

    if last_err:
        raise last_err
    raise RuntimeError(f"无法读取文件: {path}")


def ensure_stock_code_utf8(df: pl.DataFrame | None) -> pl.DataFrame | None:
    """
    确保存在标准化的 stock_code 列（与草稿脚本一致）。
    """
    if df is None:
        return None
    candidate_cols = ["stock_code", "ticker", "code", "security_id", "sec_code", "wind_code"]
    source_col = None
    for c in candidate_cols:
        if c in df.columns:
            source_col = c
            break
    if source_col is None:
        return df
    return df.with_columns(
        pl.col(source_col)
        .cast(pl.Utf8)
        .map_elements(normalize_stock_code, return_dtype=pl.Utf8)
        .alias("stock_code")
    )


def build_file_map(directory: Path) -> dict[str, Path]:
    """
    扫描目录，返回 {stock_code: file_path} 映射，不读取文件内容。
    """
    mapping: dict[str, Path] = {}
    if not directory.exists():
        return mapping

    for path in directory.glob("*.csv*"):
        name = path.name
        if name.endswith(".csv.gz"):
            stock_code = name[:-7]
        elif name.endswith(".csv"):
            stock_code = name[:-4]
        else:
            continue
        mapping[normalize_stock_code(stock_code)] = path

    return mapping


def _ensure_tradable_arrays_loaded() -> None:
    """
    懒加载可交易矩阵和对应的股票/日期信息：
    - 可交易矩阵：形状约为 (n_dates, n_stocks)，元素为 0/1
    - ticker_names：长度为 n_stocks
    - dates：长度为 n_dates
    """
    global _TRADABLE_MATRIX, _TICKER_NAMES, _DATES_8STR, _DATE_INDEX_MAP

    if _TRADABLE_MATRIX is not None and _TICKER_NAMES is not None and _DATES_8STR is not None:
        return

    try:
        # allow_pickle=True：兼容历史数据里 ticker_names / dates 为 object/bytes 的情况
        tradable = np.load(TRADABLE_NPY_PATH, allow_pickle=True)
        ticker_names_raw = np.load(TICKER_NAMES_NPY_PATH, allow_pickle=True)
        dates_raw = np.load(DATES_NPY_PATH, allow_pickle=True)
    except Exception as e:  # noqa: PERF203
        logger.error(f"加载可交易股票 numpy 文件失败: {e}")
        raise

    # 统一为二维矩阵
    tradable = np.asarray(tradable)
    if tradable.ndim != 2:
        raise ValueError(f"可交易股票矩阵维度异常: {tradable.shape}")

    # 统一 ticker 名称为字符串，并做一次 normalize_stock_code
    tickers: list[str] = []
    for t in ticker_names_raw:
        s = _to_text(t).strip()
        tickers.append(normalize_stock_code(s))

    # 统一日期为 8 位字符串 YYYYMMDD
    # 注意：不要丢弃 dates_raw 的位置（idx），否则 idx 会与矩阵轴对不上
    dates_8: list[str] = ["" for _ in range(len(dates_raw))]
    date_index_map: dict[str, int] = {}
    for idx, d in enumerate(dates_raw):
        s = _to_text(d).strip()
        if "-" in s:
            # 形如 2024-01-02，去掉 -
            s_digits = s.replace("-", "")
        else:
            s_digits = s
        # 只保留前 8 位数字
        s_digits = "".join(ch for ch in s_digits if ch.isdigit())[:8]
        if len(s_digits) != 8 or not s_digits.isdigit():
            logger.warning(f"dates.npy 中存在无法识别的日期: {d} -> {s_digits}")
            continue
        dates_8[idx] = s_digits
        # 若存在重复日期，保留第一次出现的索引即可
        date_index_map[s_digits] = idx

    # 约定：可交易矩阵固定为 (stocks, dates)
    n_stocks = len(tickers)
    n_dates = len(dates_8)
    expected_shape = (n_stocks, n_dates)
    if tradable.shape != expected_shape:
        raise ValueError(
            f"可交易矩阵形状异常: got={tradable.shape}, expected={expected_shape} "
            f"(stocks={n_stocks}, dates={n_dates})"
        )

    _TRADABLE_MATRIX = tradable
    _TICKER_NAMES = tickers
    _DATES_8STR = dates_8
    _DATE_INDEX_MAP = date_index_map


def get_tradable_stocks(date: str) -> set[str]:
    """
    从 numpy 文件获取指定日期可交易股票列表：
    - 可交易矩阵可视为一个 0/1 的二维数组，行维为日期，列维为股票
    - dates.npy 中的日期与 rows 对应，ticker_names.npy 中的股票代码与 columns 对应
    - 对于给定 date（YYYYMMDD），取对应行中值为 1 的列，得到当日可交易股票
    """
    _ensure_tradable_arrays_loaded()

    assert _TRADABLE_MATRIX is not None
    assert _TICKER_NAMES is not None
    assert _DATE_INDEX_MAP is not None

    # 统一 date 为 8 位数字字符串
    if len(date) == 10 and "-" in date:
        date8 = date.replace("-", "")
    else:
        date8 = date
    if not (len(date8) == 8 and date8.isdigit()):
        logger.warning(f"传入的日期格式无法识别为 YYYYMMDD: {date}")
        return set()

    idx = _DATE_INDEX_MAP.get(date8)
    if idx is None:
        logger.warning(f"在 dates.npy 中找不到日期 {date8}，跳过")
        return set()

    # 取出该日期对应的可交易标记列（矩阵为 [stock, date]）
    col = _TRADABLE_MATRIX[:, idx]
    if col.shape[0] != len(_TICKER_NAMES):
        logger.warning(
            f"日期 {date8} 对应列长度 {col.shape[0]} 与 ticker 数量 {len(_TICKER_NAMES)} 不一致"
        )

    tradable_set: set[str] = set()
    for flag, ticker in zip(col, _TICKER_NAMES):
        try:
            v = int(flag)
        except Exception:  # noqa: PERF203
            continue
        if v == 1:
            tradable_set.add(ticker)
    return tradable_set


def scan_available_dates() -> list[str]:
    """扫描 level2_data 目录，获取存在 Tick/Order/Trade 三表的日期列表。"""
    dates: list[str] = []
    if not LEVEL2_DATA_DIR.exists():
        return dates

    for date_dir in LEVEL2_DATA_DIR.iterdir():
        if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
            tick_dir = date_dir / "tick"
            order_dir = date_dir / "order"
            trade_dir = date_dir / "trade"
            if tick_dir.exists() and order_dir.exists() and trade_dir.exists():
                dates.append(date_dir.name)
    return sorted(dates)


def _extract_cancel_events_for_stock(
    stock_code: str,
    date: str,
    order_df: pl.DataFrame | None,
    trade_df: pl.DataFrame | None,
) -> pl.DataFrame | None:
    """
    对单只股票，基于 Order / Trade 表提取“撤单事件”（不依赖预处理结果），
    同时关联到对应的原始委托记录。

    输出列（事件级别）主要包括：
      stock_code, date,
      cancel_time_dt,              # 撤单发生时间（Datetime[ns]）
      direction,                   # 原始委托方向（Buy / Sell）
      event_volume,                # 撤单量
      order_time_dt                # 原始委托下单时间（Datetime[ns]）
    以及若干辅助列（cancel_source, 价格等），供后续扩展。
    """
    if (order_df is None or order_df.is_empty()) and (trade_df is None or trade_df.is_empty()):
        return None

    stock_code_norm = normalize_stock_code(stock_code)

    cancel_parts: list[pl.DataFrame] = []

    # ========== 1. 上交所撤单：直接在 Order 表中识别 ==========
    if order_df is not None and not order_df.is_empty():
        od = order_df
        if "stock_code" in od.columns:
            od = od.filter(pl.col("stock_code") == stock_code_norm)

        if not od.is_empty() and "order_id" in od.columns and "status" in od.columns:
            od = od.with_columns(pl.col("status").cast(pl.Utf8).alias("_status_str"))

            cancel_codes = ["Cancelled", "8", "4"]
            cancel_mask = pl.col("_status_str").is_in(cancel_codes)

            # 原始委托记录（非撤单）
            base_orders = od.filter(~cancel_mask)
            # 撤单记录
            cancel_orders = od.filter(cancel_mask)

            if not cancel_orders.is_empty() and not base_orders.is_empty():
                # 为防止同一 order_id 多条记录，原始委托记录取最早的 order_time 对应的那条
                base_orders_keyed = (
                    base_orders.sort("order_time")
                    .group_by("order_id", maintain_order=False)
                    .agg(
                        [
                            # 统一使用 order_time 作为原始委托时间来源
                            pl.first("order_time").alias("order_time"),
                            pl.first("price").alias("order_price"),
                            pl.first("volume").alias("order_volume"),
                            pl.first("direction").alias("order_direction"),
                        ]
                    )
                    )

                cancel_orders = cancel_orders.join(
                    base_orders_keyed,
                    on="order_id",
                    how="left",
                )

                # 生成撤单事件表（上交所）
                sse_cancel = cancel_orders.with_columns(
                    [
                        pl.lit(stock_code_norm).alias("stock_code"),
                        pl.lit(date).alias("date"),
                        pl.lit("SSE_ORDER").alias("cancel_source"),
                    ]
                )

                # 撤单时间 / 原始委托时间统一解析：
                # - 仅使用订单级时间字段（order_time / trade_time）
                # - 撤单事件时间：撤单记录自身的 order_time -> cancel_time_dt
                # - 原始委托时间：base_orders_keyed 中的 order_time -> order_time_dt
                sse_cancel = sse_cancel.with_columns(
                    [
                        _build_local_dt_from_time_like(date, pl.col("order_time")).alias(
                            "cancel_time_dt"
                        ),
                        _build_local_dt_from_time_like(
                            date,
                            pl.col("order_time"),
                        ).alias("order_time_dt"),
                    ]
                )

                # 标准化为“撤单事件表”所需列
                # 注意：为了兼容深交所（trade 中不一定有下单价/量）的情况，这里统一使用 Order 里的价格和数量
                sse_cancel = sse_cancel.select(
                    [
                        "stock_code",
                        "date",
                        pl.col("cancel_time_dt"),
                        pl.col("order_time_dt"),
                        pl.col("order_direction").cast(pl.Utf8).alias("direction"),
                        # 统一：用原始委托的数量作为撤单事件数量
                        pl.col("order_volume").alias("event_volume"),
                        # 预留字段
                        pl.lit("SSE_ORDER").alias("cancel_source"),
                        # 统一：用原始委托的价格作为撤单价格
                        pl.col("order_price").alias("cancel_price"),
                    ]
                )

                cancel_parts.append(sse_cancel)

    # ========== 2. 深交所撤单：从 Trade 表识别，再去 Order 表找原始委托 ==========
    if trade_df is not None and not trade_df.is_empty():
        td = trade_df
        if "stock_code" in td.columns:
            td = td.filter(pl.col("stock_code") == stock_code_norm)

        if (
            not td.is_empty()
            and "buy_id" in td.columns
            and "sell_id" in td.columns
            and "price" in td.columns
            and "volume" in td.columns
        ):
            cancel_mask = (pl.col("buy_id") == -1) | (pl.col("sell_id") == -1)
            cancel_from_trade = td.filter(cancel_mask)

            if not cancel_from_trade.is_empty() and order_df is not None and not order_df.is_empty():
                # main_id：真正被撤的订单 ID
                cancel_from_trade = cancel_from_trade.with_columns(
                    pl.when(pl.col("buy_id") == -1)
                    .then(pl.col("sell_id"))
                    .otherwise(pl.col("buy_id"))
                    .alias("main_id")
                )

                # 从 Order 表中取出对应 main_id 的原始委托记录
                od2 = order_df
                if "stock_code" in od2.columns:
                    od2 = od2.filter(pl.col("stock_code") == stock_code_norm)

                    # polars 0.20+ 中，直接传入 Series 给 is_in 会触发弃用告警；
                    # 这里显式转为 Python list，兼容新版本语义
                    main_ids = cancel_from_trade["main_id"].to_list()
                    base_orders2 = od2.filter(pl.col("order_id").is_in(main_ids))
                if not base_orders2.is_empty():
                    base_orders2_keyed = (
                        base_orders2.sort("order_time")
                        .group_by("order_id", maintain_order=False)
                        .agg(
                            [
                                # 深交所同样统一使用 order_time 作为原始委托时间来源
                                pl.first("order_time").alias("order_time"),
                                pl.first("price").alias("order_price"),
                                pl.first("volume").alias("order_volume"),
                                pl.first("direction").alias("order_direction"),
                            ]
                        )
                    )

                    cancel_from_trade = cancel_from_trade.join(
                        base_orders2_keyed,
                        left_on="main_id",
                        right_on="order_id",
                        how="left",
                    )

                    szse_cancel = cancel_from_trade.with_columns(
                        [
                            pl.lit(stock_code_norm).alias("stock_code"),
                            pl.lit(date).alias("date"),
                            pl.lit("SZSE_TRADE").alias("cancel_source"),
                        ]
                    )

                    # 同样支持 UTC 纳秒时间戳与 HHMMSSmmm（如 92317250）两种时间编码；
                    # 撤单事件时间：使用 trade 中的 trade_time -> cancel_time_dt
                    # 原始委托时间：base_orders2_keyed 中的 order_time -> order_time_dt
                    szse_cancel = szse_cancel.with_columns(
                        [
                            _build_local_dt_from_time_like(date, pl.col("trade_time")).alias(
                                "cancel_time_dt"
                            ),
                            _build_local_dt_from_time_like(
                                date,
                                pl.col("order_time"),
                            ).alias("order_time_dt"),
                        ]
                    )

                    # 深交所：trade 里不一定有下单价和委托量，这里统一用 join 过来的 Order 信息作为撤单事件价/量；
                    # 对于在 Order 表中找不到原始委托的记录（order_time_dt 为空），直接丢弃。
                    # 这里保留 main_id，方便你在调试时对照 Trade 里的撤单记录。
                    szse_cancel = (
                        szse_cancel.select(
                            [
                                "stock_code",
                                "date",
                                pl.col("cancel_time_dt"),
                                pl.col("order_time_dt"),
                                pl.col("order_direction").cast(pl.Utf8).alias("direction"),
                                # 统一：用原始委托的数量作为撤单事件数量
                                pl.col("order_volume").alias("event_volume"),
                                # 预留字段
                                pl.lit("SZSE_TRADE").alias("cancel_source"),
                                # 统一：用原始委托的价格作为撤单价格
                                pl.col("order_price").alias("cancel_price"),
                                # 调试用：被撤的订单 ID
                                pl.col("main_id").alias("main_id"),
                            ]
                        ).filter(pl.col("order_time_dt").is_not_null())
                    )

                    cancel_parts.append(szse_cancel)

    if not cancel_parts:
        return None

    if len(cancel_parts) == 1:
        result = cancel_parts[0]
    else:
        result = pl.concat(cancel_parts, how="vertical")

    # 显式释放中间列表，减轻内存压力
    del cancel_parts
    return result


def _build_minute_features_from_cancel_events(df_cancel: pl.DataFrame) -> pl.DataFrame | None:
    """
    在“撤单事件表”基础上，构造分钟级撤单统计特征。

    期望输入列：
      stock_code, date, cancel_time_dt, direction, event_volume, order_time_dt(必须)

    输出列（分钟级）主要包括：
      - cancel_buy_count,  cancel_sell_count
      - cancel_buy_volume, cancel_sell_volume
      - algo_cancel_buy_count,  algo_cancel_sell_count
      - algo_cancel_buy_volume, algo_cancel_sell_volume
    """
    if df_cancel is None or df_cancel.is_empty():
        return None

    # order_time_dt 是算法撤单识别所需的关键字段：Δ = T_cancel - T_order（毫秒）
    required_cols = [
        "stock_code",
        "date",
        "cancel_time_dt",
        "direction",
        "event_volume",
        "order_time_dt",
    ]
    missing = [c for c in required_cols if c not in df_cancel.columns]
    if missing:
        logger.error(
            f"撤单事件数据缺少必要列: {missing}。其中 order_time_dt 为必须列，"
            f"请在上游撤单事件构造阶段补齐（例如在 Order/Trade join 原始委托时带出）。"
        )
        return None

    # 统一时间轴：使用 cancel_time_dt（Datetime[ns]），构造分钟桶
    df_cancel = df_cancel.with_columns(
        [
            # minute: 当分钟的起始时间，例如 09:31:12.345 -> 09:31:00
            pl.col("cancel_time_dt").dt.truncate("1m").alias("minute"),
        ]
    )

    # 统一在本函数内计算 is_algo_like（不依赖/不复用外部列），并直接覆盖写入。
    centers_cfg = getattr(config, "ALGO_PULSE_CENTERS_MS", [])
    half_width = int(getattr(config, "ALGO_PULSE_HALF_WIDTH_MS", 0) or 0)

    if not centers_cfg or half_width <= 0:
        logger.warning(
            "未配置 ALGO_PULSE_CENTERS_MS 或 ALGO_PULSE_HALF_WIDTH_MS<=0，"
            "无法进行算法撤单识别，将全部记为非算法撤单（is_algo_like=False）。"
        )
        df_cancel = df_cancel.with_columns(pl.lit(False).alias("is_algo_like"))
    else:
        # 在当前 Polars 版本中，DurationExpr.dt.milliseconds / .nanoseconds 等接口不可用，
        # 这里退而求其次，先把 datetime 转为底层整数时间戳（ns），再做差并换算成毫秒：
        #   1) pl.col("xxx_dt").cast(pl.Int64)   -> 以纳秒为单位的时间戳（int）
        #   2) 二者相减得到纳秒差值
        df_cancel = df_cancel.with_columns(
            (
                pl.col("cancel_time_dt").cast(pl.Int64)
                - pl.col("order_time_dt").cast(pl.Int64)
            ).alias("_delta_ms_from_order")
        )

        # 时间窗口 W：使用 config 中的 algo_time_window_ms（默认 1 分钟）
        window_ms = int(getattr(config, "algo_time_window_ms", 60_000))

        # 最终使用的脉冲中心：完全由配置 ALGO_PULSE_CENTERS_MS 决定（毫秒）
        all_centers = sorted({int(c) for c in centers_cfg})

        # 先筛掉超过窗口的撤单：Δ + half_width > W 视为非算法
        in_window_expr = (
            (pl.col("_delta_ms_from_order").is_not_null())
            & ((pl.col("_delta_ms_from_order") + half_width) <= window_ms)
        )

        # 对每个时间脉冲中心构造一个布尔条件，最后做“或”逻辑
        # 例如：Δ_ms=1000 对应 “+1 秒”，若撤单时间在 [1000-20, 1000+20]ms 即命中
        conds = [
            (pl.col("_delta_ms_from_order") >= center - half_width)
            & (pl.col("_delta_ms_from_order") <= center + half_width)
            for center in all_centers
        ]
        pulse_hit_expr = pl.any_horizontal(conds).fill_null(False)
        is_algo_expr = (in_window_expr & pulse_hit_expr).alias("is_algo_like")

        # df_cancel = df_cancel.with_columns(is_algo_expr).drop(
        #     ["_delta_ms_from_order"], strict=False
        # )
        df_cancel = df_cancel.with_columns(is_algo_expr)

    # 方向列统一为字符串（"Buy"/"Sell"/其它）
    df_cancel = df_cancel.with_columns(
        pl.col("direction").cast(pl.Utf8)
    )

    # 分钟 + 股票 + 日期维度聚合
    gb_cols = ["stock_code", "date", "minute"]

    # 定义便捷表达式
    is_buy = pl.col("direction") == "Buy"
    is_sell = pl.col("direction") == "Sell"
    is_algo = pl.col("is_algo_like")

    agg_df = df_cancel.group_by(gb_cols, maintain_order=False).agg(
        [
            # 买卖撤单笔数
            (is_buy.cast(pl.Int64).sum()).alias("cancel_buy_count"),
            (is_sell.cast(pl.Int64).sum()).alias("cancel_sell_count"),

            # 买卖撤单量
            pl.when(is_buy).then(pl.col("event_volume")).otherwise(0).sum().alias(
                "cancel_buy_volume"
            ),
            pl.when(is_sell).then(pl.col("event_volume")).otherwise(0).sum().alias(
                "cancel_sell_volume"
            ),

            # 算法买卖撤单笔数
            (is_buy & is_algo).cast(pl.Int64).sum().alias("algo_cancel_buy_count"),
            (is_sell & is_algo).cast(pl.Int64).sum().alias("algo_cancel_sell_count"),

            # 算法买卖撤单量
            pl.when(is_buy & is_algo)
            .then(pl.col("event_volume"))
            .otherwise(0)
            .sum()
            .alias("algo_cancel_buy_volume"),
            pl.when(is_sell & is_algo)
            .then(pl.col("event_volume"))
            .otherwise(0)
            .sum()
            .alias("algo_cancel_sell_volume"),
        ]
    )

    # 排序便于后续查看
    agg_df = agg_df.sort(["stock_code", "minute"])

    # df_cancel 已不再使用，显式删除以便早点释放
    del df_cancel
    return agg_df


def _filter_dates_by_config(dates: Iterable[str]) -> list[str]:
    """按 config.START_DATE / END_DATE 过滤日期列表。"""
    res: List[str] = []
    start = str(config.START_DATE) if getattr(config, "START_DATE", None) else None
    end = str(config.END_DATE) if getattr(config, "END_DATE", None) else None

    for d in dates:
        if start and d < start:
            continue
        if end and d > end:
            continue
        res.append(d)
    return res


def _process_single_stock_task(task: tuple[str, str, Path | None, Path | None]) -> Path | None:
    """
    供多进程调用的单股票处理任务：
    - 读取对应股票的 Order / Trade
    - 提取撤单事件
    - 在撤单事件上构造该股票当日的分钟级因子
    - 直接将分钟因子写出为临时 parquet 文件

    返回值：
    - 写出的单股票分钟因子 parquet 路径（Path），若无有效数据则返回 None
    """
    stock_code, date, order_path, trade_path = task

    # 统一规范化股票代码，保证文件名一致
    stock_code_norm = normalize_stock_code(stock_code)

    if not order_path and not trade_path:
        return None

    order_df = trade_df = None
    cancel_df: pl.DataFrame | None = None
    minute_df: pl.DataFrame | None = None
    try:
        if order_path:
            order_df = ensure_stock_code_utf8(read_csv_auto(order_path))
        if trade_path:
            trade_df = ensure_stock_code_utf8(read_csv_auto(trade_path))

        # 1) 单股撤单事件
        cancel_df = _extract_cancel_events_for_stock(
            stock_code_norm,
            date,
            order_df,
            trade_df,
        )
        if cancel_df is None or cancel_df.is_empty():
            return None

        # 2) 在撤单事件上直接构造该股票当日的分钟因子
        minute_df = _build_minute_features_from_cancel_events(cancel_df)
        if minute_df is None or minute_df.is_empty():
            return None

        # 3) 写出到单股票分钟级临时目录：{PER_STOCK_MINUTE_DIR}/{date}/{stock_code}.parquet
        out_dir = PER_STOCK_MINUTE_DIR / date
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{stock_code_norm}.parquet"
        minute_df.write_parquet(out_path)

        # 显式删除中间结果，减轻 worker 进程内存压力
        del cancel_df, minute_df
        gc.collect()

        return out_path
    except Exception as e:  # noqa: PERF203
        logger.warning(f"处理股票失败（date={date}, stock={stock_code_norm}）: {e}")
        return None
    finally:
        # 显式删除原始订单/成交表，减轻 worker 进程内存压力
        del order_df, trade_df


def process_single_date(date: str, global_pbar: tqdm | None = None) -> bool:
    """
    处理单个交易日：
    - 读取当日全市场的 Order / Trade（按股票拆分文件）
    - 多进程并行：按股票直接构造“单股分钟因子”并写出临时 parquet
    - 主进程在该交易日所有股票分钟因子基础上做一次全市场合并，并写出最终结果
    """
    # 若配置了 START_DATE / END_DATE，则在单日入口也做一次日期过滤，
    # 不在区间内的日期直接跳过，避免无效计算。
    start = str(config.START_DATE) if getattr(config, "START_DATE", None) else None
    end = str(config.END_DATE) if getattr(config, "END_DATE", None) else None
    if (start and date < start) or (end and date > end):
        msg = f"日期 {date} 不在配置区间 [{start or '-∞'}, {end or '+∞'}] 内，跳过"
        logger.info(msg)
        return False

    logger.info(f"处理日期: {date}")

    # 1. 可交易股票列表
    tradable_stocks = get_tradable_stocks(date)
    if not tradable_stocks:
        msg = f"  警告：日期 {date} 没有可交易股票，跳过"
        logger.warning(f"日期 {date} 无可交易股票，跳过")
        return False

    date_dir = LEVEL2_DATA_DIR / date
    order_map = build_file_map(date_dir / "order")
    trade_map = build_file_map(date_dir / "trade")

    if not order_map or not trade_map:
        msg = (
            f"  警告：日期 {date} 的 Order/Trade 目录缺失文件，"
            f"Order={len(order_map)}, Trade={len(trade_map)}，跳过"
        )
        logger.warning(
            f"日期 {date} 的 Order/Trade 目录缺失文件，Order={len(order_map)}, Trade={len(trade_map)}，跳过"
        )
        return False

    all_stock_codes = set(order_map) | set(trade_map)
    target_stocks = sorted(tradable_stocks & all_stock_codes)
    if not target_stocks:
        # 打印少量样例，便于快速定位“代码格式不一致”等问题
        sample_tradable = sorted(tradable_stocks)[:10]
        sample_files = sorted(all_stock_codes)[:10]
        msg = (
            f"  警告：日期 {date} 无匹配股票，跳过（tradable={len(tradable_stocks)}, files={len(all_stock_codes)}）\n"
            f"        tradable样例: {sample_tradable}\n"
            f"        文件样例: {sample_files}"
        )
        logger.warning(
            f"日期 {date} 无匹配股票，跳过（tradable={len(tradable_stocks)}, files={len(all_stock_codes)}）"
        )
        return False

    # 2. 组装多进程任务
    tasks: list[tuple[str, str, Path | None, Path | None]] = []
    for stock_code in target_stocks:
        tasks.append(
            (
                stock_code,
                date,
                order_map.get(stock_code),
                trade_map.get(stock_code),
            )
        )

    # 单股票分钟因子临时文件路径列表
    minute_file_paths: list[Path] = []

    # 若传入全局进度条，则只负责 update，不再在此处修改 total，
    # 以免出现“第二个日期一开始就是 xx%”的视觉错觉。
    # 当前版本中 process_all_dates 不再传入全局进度条，该分支暂不会命中，
    # 保留代码仅作为后续扩展预留。
    if global_pbar is not None:
        global_pbar.set_postfix_str(f"date={date}", refresh=False)
        global_pbar.refresh()

    # 多进程并行股票维度，Polars 内部线程数锁死为 1，避免过度竞争。
    # 优先使用全局配置中的 N_JOBS；若未配置或配置非法，则退回为 (CPU 数 - 1)。
    n_workers_cfg = getattr(config, "N_JOBS", None)
    try:
        n_workers = int(n_workers_cfg) if n_workers_cfg is not None else 0
    except Exception:
        n_workers = 0
    if n_workers <= 0:
        n_workers = max(mp.cpu_count() - 1, 1)

    # 不必开超过任务数的进程
    n_workers = max(1, min(n_workers, len(tasks)))

    # 若仅 1 个 worker，则在当前进程顺序执行任务
    if n_workers == 1:
        if global_pbar is None:
            pbar = tqdm(
                total=len(tasks),
                desc=f"{date} 股票处理(单进程)",
                leave=True,
                dynamic_ncols=True,
                mininterval=0.5,
                file=sys.stderr,
            )
        else:
            pbar = global_pbar

        for task in tasks:
            res = _process_single_stock_task(task)
            if res is not None:
                minute_file_paths.append(res)
            pbar.update(1)

        if global_pbar is None:
            pbar.close()

    # ========== 分支2：多进程 ==========
    else:
        # 为该日期单独显示一个从 0% 开始的进度条
        if global_pbar is None:
            pbar = tqdm(
                total=len(tasks),
                desc=f"{date} 股票处理({n_workers}进程)",
                leave=True,
                dynamic_ncols=True,
                mininterval=0.5,
                file=sys.stderr,
            )
        else:
            pbar = global_pbar

        # 使用 ProcessPoolExecutor + as_completed，参考草稿脚本的鲁棒模式，
        # 便于在未来按股票引入超时/失败隔离。
        try:
            with ProcessPoolExecutor(max_workers=n_workers) as executor:
                future_map = {
                    executor.submit(_process_single_stock_task, task): task
                    for task in tasks
                }

                finished_cnt = 0
                for fut in as_completed(future_map):
                    task = future_map[fut]
                    stock_code, _, _, _ = task
                    try:
                        res = fut.result()
                        finished_cnt += 1
                        if res is not None:
                            minute_file_paths.append(res)
                    except Exception as e:  # noqa: PERF203
                        logger.warning(
                            f"处理股票失败（多进程，date={date}, stock={stock_code}）: {e}"
                        )
                    finally:
                        pbar.update(1)

                if finished_cnt < len(future_map):
                    logger.warning(
                        f"日期 {date} 多进程执行结束，但仅完成 "
                        f"{finished_cnt}/{len(future_map)} 个任务"
                    )
        except KeyboardInterrupt:
            # 捕获 Ctrl+C，尽可能优雅地关闭进程池
            logger.warning("收到 KeyboardInterrupt，中断当前日期的多进程任务")
            raise
        finally:
            if global_pbar is None:
                pbar.close()

    # 任务列表已不再需要，显式删除
    del tasks

    if not minute_file_paths:
        msg = f"  日期 {date} 无可用单股分钟因子，跳过写出"
        logger.info(f"日期 {date} 无可用单股分钟因子，跳过")
        return False

    # 汇总当日所有股票的分钟因子（全市场级别）
    # 这里直接读取各单股分钟 parquet 并 concat，数据量相对撤单事件已大幅压缩
    try:
        minute_parts: list[pl.DataFrame] = [pl.read_parquet(p) for p in minute_file_paths]
    except Exception as e:  # noqa: PERF203
        msg = f"  日期 {date} 读取单股分钟因子失败: {e}"
        logger.exception(msg)
        return False

    minute_df = (
        pl.concat(minute_parts, how="vertical") if len(minute_parts) > 1 else minute_parts[0]
    )
    del minute_parts

    if minute_df is None or minute_df.is_empty():
        msg = f"  日期 {date} 合并后无可用撤单分钟数据"
        logger.info(f"日期 {date} 合并后无可用撤单分钟数据")

        # 显式删除中间结果
        del minute_df
        gc.collect()
        return False

    # 写出前强制排序，确保日频分钟级 parquet 内部顺序稳定：stock_code, minute
    # minute 为 Datetime，按时间自然排序；stock_code 为 6 位字符串
    minute_df = minute_df.sort(["stock_code", "minute"])

    # 因子缓存根目录下的“日频分钟级”子目录；如不存在则创建
    per_minute_dir = CANCEL_MINUTE_DIR / "日频分钟级"
    per_minute_dir.mkdir(parents=True, exist_ok=True)
    out_file = per_minute_dir / f"{date}.parquet"
    minute_df.write_parquet(out_file)

    # 写盘后删除大对象，减轻内存压力
    del minute_df
    gc.collect()

    # 清理单股票临时 parquet，减少磁盘占用
    for p in minute_file_paths:
        try:
            if p.exists():
                p.unlink()
        except Exception as e:  # noqa: PERF203
            logger.warning(f"删除临时单股分钟因子文件失败: {p}, err={e}")

    # 若当日子目录已空，则尝试删除该目录
    per_stock_dir = PER_STOCK_MINUTE_DIR / date
    try:
        if per_stock_dir.exists() and not any(per_stock_dir.iterdir()):
            per_stock_dir.rmdir()
    except Exception as e:  # noqa: PERF203
        logger.warning(f"删除单日临时目录失败: {per_stock_dir}, err={e}")
    msg = f"  写出分钟聚合文件: {out_file}"
    logger.info(f"写出分钟聚合文件: {out_file}")
    return True


def process_all_dates() -> None:
    """
    主入口：遍历 level2_data 中已有日期，直接从原始三表构造撤单分钟因子。
    """
    print("=" * 60)
    print("step1_数据处理：原始逐笔 -> 撤单事件 -> 分钟因子")
    print("=" * 60)
    logger.info("step1_数据处理脚本启动")

    dates = scan_available_dates()
    if not dates:
        print("未在 level2_data 目录下发现任何日期，退出")
        logger.error("未发现 level2_data 日期")
        return

    dates = _filter_dates_by_config(dates)
    if not dates:
        print("按 config 日期过滤后无可用日期，退出")
        logger.error("按配置过滤后无可用日期")
        return

    logger.info(f"待处理日期数: {len(dates)}")

    success = 0
    skipped = 0
    failed = 0

    # 改为“按日期重置”的进度条：每个 date 都会单独显示一个从 0% 开始的 bar
    for date in dates:
        try:
            ok = process_single_date(date, global_pbar=None)
            if ok:
                success += 1
            else:
                skipped += 1
        except Exception as e:  # noqa: PERF203
            tqdm.write(f"  日期 {date} 处理失败: {e}")
            logger.exception(f"日期 {date} 处理失败: {e}")
            failed += 1

    print("\n" + "=" * 60)
    # print(f"step1_数据处理完成：成功写出={success}，跳过={skipped}，失败={failed}")
    print("=" * 60)
    logger.info(f"step1_数据处理完成：成功写出={success}，跳过={skipped}，失败={failed}")

    # 在所有交易日分钟级文件写出完成后，汇总为“全市场全时间”的日频基础数据.pkl
    try:
        _build_full_market_daily_pickle_from_minutes()
    except Exception as e:  # noqa: PERF203
        print(f"汇总日频分钟级数据为全量 pkl 失败: {e}")
        logger.exception(f"汇总日频分钟级数据为全量 pkl 失败: {e}")


def _build_full_market_daily_pickle_from_minutes() -> None:
    """
    扫描因子缓存/日频分钟级 下所有按日写出的分钟级 parquet，
    先纵向 concat，再按“日期 + 股票”做字段求和聚合，得到日频数据，并保存为：
        因子缓存/全量/cancel_minute_all.pkl

    说明：
    - 聚合后，每个 (date, stock_code) 只有一行；
    - 对所有数值型字段做求和聚合（分钟级上的加总），
      例如 cancel_buy_count, algo_cancel_buy_volume 等；
    - 非数值字段（如 minute, is_algo_like 等）在日频层面不再保留。
    """
    per_minute_dir = CANCEL_MINUTE_DIR / "日频分钟级"
    if not per_minute_dir.exists():
        print(f"未找到日频分钟级目录: {per_minute_dir}，跳过全量 pkl 汇总")
        logger.warning(f"未找到日频分钟级目录: {per_minute_dir}，跳过全量 pkl 汇总")
        return

    files = sorted(per_minute_dir.glob("*.parquet"))
    if not files:
        print(f"日频分钟级目录 {per_minute_dir} 下无 parquet 文件，跳过全量 pkl 汇总")
        logger.warning(f"日频分钟级目录 {per_minute_dir} 下无 parquet 文件，跳过全量 pkl 汇总")
        return

    print(f"开始将 {len(files)} 个日频分钟级 parquet 汇总为全量 pkl ...")
    logger.info(f"开始汇总日频分钟级 parquet，文件数={len(files)}")

    parts: list[pl.DataFrame] = []
    for p in tqdm(files, desc="读取日频分钟级 parquet", dynamic_ncols=True, leave=False):
        try:
            df = pl.read_parquet(p)
            if df is not None and not df.is_empty():
                parts.append(df)
        except Exception as e:  # noqa: PERF203
            logger.warning(f"读取日频分钟级文件失败，已跳过: {p}, err={e}")

    if not parts:
        print("所有日频分钟级 parquet 均为空或读取失败，未生成全量 pkl")
        logger.warning("所有日频分钟级 parquet 均为空或读取失败，未生成全量 pkl")
        return

    all_df = pl.concat(parts, how="vertical") if len(parts) > 1 else parts[0]
    del parts

    if all_df.is_empty():
        print("汇总后的日频分钟级数据为空，未生成全量 pkl")
        logger.warning("汇总后的日频分钟级数据为空，未生成全量 pkl")
        return

    # 分组键：日期 + 股票；这里假定分钟级 parquet 中已经包含这两列
    group_keys = ["date", "stock_code"]
    missing_keys = [k for k in group_keys if k not in all_df.columns]
    if missing_keys:
        print(f"分钟级数据缺少必要分组键列，无法做日频聚合: {missing_keys}")
        logger.error(f"分钟级数据缺少必要分组键列，无法做日频聚合: {missing_keys}")
        return

    # 只对数值型字段做求和聚合；非数值列在日频输出中丢弃
    daily_df = (
        all_df
        .group_by(group_keys, maintain_order=False)
        .agg(pl.col(pl.NUMERIC_DTYPES).sum())
    )
    del all_df

    if daily_df.is_empty():
        print("日频聚合后的数据为空，未生成全量 pkl")
        logger.warning("日频聚合后的数据为空，未生成全量 pkl")
        return

    # 转为 pandas 后写出 pkl，方便后续下游因子脚本直接使用
    pdf = daily_df.to_pandas()
    del daily_df

    # 统一交易日为 datetime 并排序：
    # - date: "YYYYMMDD" -> datetime64[ns]
    # - 按 date（交易时间）+ stock_code 排序，保证输出顺序稳定
    if "date" in pdf.columns:
        pdf["date"] = pd.to_datetime(pdf["date"].astype(str), format="%Y%m%d", errors="coerce")
    if "stock_code" in pdf.columns:
        pdf["stock_code"] = pdf["stock_code"].astype(str)
    sort_cols = [c for c in ["date", "stock_code"] if c in pdf.columns]
    if sort_cols:
        pdf = pdf.sort_values(sort_cols, ascending=True, kind="mergesort").reset_index(drop=True)

    full_dir = CANCEL_MINUTE_DIR / "全量"
    full_dir.mkdir(parents=True, exist_ok=True)
    out_path = full_dir / "撤单全量数据.pkl"

    pdf.to_pickle(out_path)
    print(f"已生成全市场全时间的日频基础数据: {out_path}")
    logger.info(f"已生成全市场全时间的日频基础数据 pkl: {out_path}")


if __name__ == "__main__":
    # 参考草稿脚本，在 Linux 上显式使用 spawn 启动方式，降低多进程+数值库死锁风险。
    try:
        if hasattr(os, "name") and os.name == "posix":
            try:
                mp.set_start_method("spawn", force=True)
                print(f"多进程启动方式: {mp.get_start_method()}", flush=True)
            except RuntimeError as e:
                print(f"警告：无法设置多进程启动方式: {e}", flush=True)
        else:
            # 非 posix 平台（如 Windows），仅打印当前启动方式
            try:
                print(f"当前多进程启动方式: {mp.get_start_method()}", flush=True)
            except Exception:
                pass
    except Exception as e:
        print(f"多进程初始化检查失败: {e}", flush=True)

    process_all_dates()

