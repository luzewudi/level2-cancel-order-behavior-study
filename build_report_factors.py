from __future__ import annotations

"""
复现《订单流系列：撤单行为规律初探》中的撤单类因子。

本脚本刻意放在项目根目录，而不是草稿目录中，作为正式可重复运行的入口：
1. 从 Level-2 order/trade 原始逐笔文件中重建订单生命周期；
2. 按全撤、部撤、废单三类订单行为计算买卖方向三小将；
3. 按 order_id 匹配原始委托时间和撤单时间，计算毒流动性；
4. 将结果写成与 EOD 完全一致的 (stock, date) npy 矩阵。
"""

import argparse
import gzip
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from numpy.lib.format import open_memmap
from tqdm.auto import tqdm

import config
from utils.log_kit import get_logger


# utils.log_kit 的控制台前缀包含 Unicode 符号；Windows PowerShell 默认 GBK
# 时可能无法输出。这里仅调整当前脚本的标准输出/错误编码，不改全局环境。
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = get_logger("build_report_factors")


MS_PER_SECOND = 1_000
MS_PER_DAY = 24 * 60 * 60 * MS_PER_SECOND
LOCAL_OFFSET_MS = 8 * 60 * 60 * MS_PER_SECOND

CANCEL_STATUS_CODES = {"Cancelled", "Cancel", "8", "4"}
DIRECTIONS = ("buy", "sell")
CANCEL_KINDS = ("all_cancel", "part_cancel", "negative")


@dataclass(frozen=True)
class TimeSegment:
    """一个可配置的日内时间段，内部统一转成“当日毫秒数”比较。"""

    name: str
    start_ms: int
    end_ms: int
    include_end: bool = False


@dataclass
class StockResult:
    """单只股票、单个交易日的中间结果。

    volumes 中存的是分类订单量，还没有除以自由流通股本；主进程拿到结果后
    再按 EOD 坐标写入最终矩阵。
    """

    stock_code: str
    volumes: dict[str, float]
    tox_5s_count: int
    tox_30s_count: int
    order_count: int
    cancel_event_count: int
    negative_order_count: int


def _parse_hms_ms(value: str) -> int:
    """把 'HH:MM:SS.mmm' 配置值转成当日毫秒，便于和 order_time 比较。"""

    hms, _, ms_part = value.partition(".")
    hh, mm, ss = [int(x) for x in hms.split(":")]
    ms = int((ms_part or "0").ljust(3, "0")[:3])
    return ((hh * 60 + mm) * 60 + ss) * 1000 + ms


SEGMENTS = [
    TimeSegment(name, _parse_hms_ms(start), _parse_hms_ms(end), include_end)
    for name, start, end, include_end in config.TIME_SEGMENTS
]
SEGMENT_NAMES = [s.name for s in SEGMENTS]


def _to_text(value: Any) -> str:
    """兼容 bytes / numpy.bytes_ / 普通对象的文本转换。"""

    try:
        if isinstance(value, (bytes, bytearray, np.bytes_)):  # type: ignore[attr-defined]
            raw = bytes(value)
            for encoding in ("utf-8", "gb18030"):
                try:
                    return raw.decode(encoding)
                except UnicodeDecodeError:
                    pass
            return raw.decode("utf-8", errors="replace")
    except Exception:
        pass
    return str(value)


def normalize_stock_code(code: Any) -> str:
    """将各种股票代码格式统一成 6 位数字字符串。"""

    text = _to_text(code).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    if digits:
        return digits.zfill(6)
    return text


def factor_names() -> list[str]:
    """根据当前 config.TIME_SEGMENTS 生成需要输出的因子文件名。"""

    names: list[str] = []
    for segment in SEGMENT_NAMES:
        for direction in DIRECTIONS:
            for kind in CANCEL_KINDS:
                names.append(f"{direction}_{kind}_rate_{segment}")
        for direction in DIRECTIONS:
            names.append(f"{direction}_tri_{segment}")
    names.append("tox_5s_over_30s")
    return names


def read_csv_auto(path: Path) -> pl.DataFrame:
    """读取 csv/csv.gz，优先 UTF-8，失败后回退 gb18030。

    这里不直接把 gzip 文件路径交给 Polars 加 encoding 参数，是因为部分 Polars
    版本会先按文本解码 gzip 二进制头，导致 0x8b 解码错误。显式 gzip.open 成
    文本流后再交给 Polars 更稳。
    """

    last_error: Exception | None = None
    for encoding, errors in (("utf-8", "strict"), ("gb18030", "replace")):
        handle = None
        try:
            if path.name.endswith(".gz"):
                handle = gzip.open(path, mode="rt", encoding=encoding, errors=errors)
                return pl.read_csv(handle, infer_schema_length=1000)
            handle = path.open(mode="r", encoding=encoding, errors=errors)
            return pl.read_csv(handle, infer_schema_length=1000)
        except Exception as exc:  # noqa: PERF203
            last_error = exc
        finally:
            if handle is not None:
                handle.close()
    assert last_error is not None
    raise last_error


def build_file_map(directory: Path) -> dict[str, Path]:
    """扫描某日 order/trade 目录，返回 {标准化股票代码: 文件路径}。"""

    if not directory.exists():
        return {}
    mapping: dict[str, Path] = {}
    for path in directory.glob("*.csv*"):
        name = path.name
        if name.endswith(".csv.gz"):
            raw_code = name[:-7]
        elif name.endswith(".csv"):
            raw_code = name[:-4]
        else:
            continue
        mapping[normalize_stock_code(raw_code)] = path
    return mapping


def infer_exchange(path: Path | None, df: pl.DataFrame | None) -> str:
    """从 stock_exchange 列或文件名推断交易所。"""

    if df is not None and "stock_exchange" in df.columns and not df.is_empty():
        value = str(df["stock_exchange"][0]).upper()
        if value in {"SSE", "SZSE"}:
            return value
    if path is not None:
        name = path.name.upper()
        if "SZSE" in name:
            return "SZSE"
        if "SSE" in name:
            return "SSE"
    return ""


def time_ms_expr(column: str) -> pl.Expr:
    """把 Level-2 时间列解析为“当日毫秒数”。

    本地数据中 order_time/trade_time 有两种常见编码：
    - 8 位：例如 91500120，表示 09:15:00.120；
    - 9 位：例如 130000000，表示 13:00:00.000。

    这两种本质都是 HHMMSSmmm，只是 09 点没有前导 0。函数会统一转成“日内毫秒数”，
    例如 09:15:00.120 会转成 33300120。若遇到 UTC epoch 纳秒时间戳，则先转成
    北京时间，再取对应的日内毫秒数。
    """

    raw = pl.col(column).cast(pl.Int64, strict=False)
    is_epoch_ns = raw.abs() >= 1_000_000_000_000
    hh = raw.abs() // 10_000_000
    mm = (raw.abs() // 100_000) % 100
    ss = (raw.abs() // 1000) % 100
    ms = raw.abs() % 1000
    hms_ms = ((hh * 60 + mm) * 60 + ss) * 1000 + ms
    epoch_local_ms = ((raw // 1_000_000) + LOCAL_OFFSET_MS) % MS_PER_DAY
    return pl.when(raw.is_null()).then(None).when(is_epoch_ns).then(epoch_local_ms).otherwise(hms_ms)


def direction_expr(column: str = "direction") -> pl.Expr:
    """统一买卖方向，输出 buy/sell；无法识别的方向置空。"""

    text = pl.col(column).cast(pl.Utf8).str.to_lowercase()
    return (
        pl.when(text.is_in(["buy", "b", "1"]))
        .then(pl.lit("buy"))
        .when(text.is_in(["sell", "s", "2"]))
        .then(pl.lit("sell"))
        .otherwise(None)
    )


def segment_expr(time_ms_column: str) -> pl.Expr:
    """按 config.TIME_SEGMENTS 把某个当日毫秒列映射到时间段名称。"""

    expr = pl.lit(None, dtype=pl.Utf8)
    t = pl.col(time_ms_column)
    for segment in reversed(SEGMENTS):
        if segment.include_end:
            cond = (t >= segment.start_ms) & (t <= segment.end_ms)
        else:
            cond = (t >= segment.start_ms) & (t < segment.end_ms)
        expr = pl.when(cond).then(pl.lit(segment.name)).otherwise(expr)
    return expr


def prepare_orders(order_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """整理 order 表，拆出原始委托记录和上交所撤单记录。

    base_orders 是订单生命周期的起点，每个 order_id 只保留最早一条非撤单记录；
    cancel_candidates 只保留 order 表中 status 表示撤单的行，主要服务上交所。
    这样可以避免把上交所撤单记录自身的 order_time 误当成原始委托时间。
    """

    required = {"order_id", "direction", "volume", "order_time"}
    missing = required - set(order_df.columns)
    if missing:
        raise ValueError(f"order data missing columns: {sorted(missing)}")

    od = order_df.with_columns(
        [
            pl.col("order_id").cast(pl.Int64, strict=False).alias("order_id"),
            pl.col("volume").cast(pl.Float64, strict=False).alias("order_volume"),
            pl.col("price").cast(pl.Float64, strict=False).alias("order_price")
            if "price" in order_df.columns
            else pl.lit(np.nan).alias("order_price"),
            direction_expr("direction").alias("direction_norm"),
            time_ms_expr("order_time").alias("order_time_ms"),
        ]
    )

    status = (
        pl.col("status").cast(pl.Utf8).is_in(CANCEL_STATUS_CODES)
        if "status" in od.columns
        else pl.lit(False)
    )
    base_candidates = od.filter(~status)
    cancel_candidates = od.filter(status)

    if base_candidates.is_empty():
        base_candidates = od

    base_orders = (
        base_candidates.filter(pl.col("order_id").is_not_null())
        .sort(["order_id", "order_time_ms"])
        .group_by("order_id", maintain_order=False)
        .agg(
            [
                pl.first("direction_norm").alias("direction"),
                pl.first("order_volume").alias("orig_volume"),
                pl.first("order_price").alias("orig_price"),
                pl.first("order_time_ms").alias("orig_time_ms"),
            ]
        )
    )
    return base_orders, cancel_candidates


def trade_volume_by_order(trade_df: pl.DataFrame | None) -> pl.DataFrame:
    """从 trade 表汇总每个 order_id 的成交量。

    一笔成交同时涉及买卖两边订单，因此需要把 buy_id 和 sell_id 展开成同一列
    后再 group_by。废单 negative 的判断正是依赖这里得到的 trade_volume。
    """

    if trade_df is None or trade_df.is_empty():
        return pl.DataFrame({"order_id": [], "trade_volume": []}, schema={"order_id": pl.Int64, "trade_volume": pl.Float64})
    required = {"buy_id", "sell_id", "volume"}
    if required - set(trade_df.columns):
        return pl.DataFrame({"order_id": [], "trade_volume": []}, schema={"order_id": pl.Int64, "trade_volume": pl.Float64})

    td = trade_df.with_columns(
        [
            pl.col("buy_id").cast(pl.Int64, strict=False).alias("buy_id"),
            pl.col("sell_id").cast(pl.Int64, strict=False).alias("sell_id"),
            pl.col("volume").cast(pl.Float64, strict=False).alias("volume"),
        ]
    )
    if "exec_type" in td.columns:
        trades = td.filter(pl.col("exec_type").cast(pl.Utf8) == "Trade")
    else:
        trades = td.filter((pl.col("buy_id") > 0) & (pl.col("sell_id") > 0))
    if trades.is_empty():
        return pl.DataFrame({"order_id": [], "trade_volume": []}, schema={"order_id": pl.Int64, "trade_volume": pl.Float64})

    order_sides = pl.concat(
        [
            trades.select(pl.col("buy_id").alias("order_id"), "volume"),
            trades.select(pl.col("sell_id").alias("order_id"), "volume"),
        ],
        how="vertical",
    )
    return (
        order_sides.filter(pl.col("order_id") > 0)
        .group_by("order_id", maintain_order=False)
        .agg(pl.col("volume").sum().alias("trade_volume"))
    )


def cancel_events_from_trade(trade_df: pl.DataFrame | None, base_orders: pl.DataFrame) -> pl.DataFrame:
    """从 trade 表提取深交所主动撤单事件。

    深交所撤单通常记录在 trade 表中，buy_id 或 sell_id 为 -1，另一侧才是真正
    被撤的 order_id。提取后立即 join base_orders，带出原始委托量、原始委托时间和方向。
    """

    schema = {
        "order_id": pl.Int64,
        "cancel_volume": pl.Float64,
        "cancel_time_ms": pl.Int64,
        "direction": pl.Utf8,
        "orig_volume": pl.Float64,
        "orig_time_ms": pl.Int64,
    }
    if trade_df is None or trade_df.is_empty() or {"buy_id", "sell_id", "volume"} - set(trade_df.columns):
        return pl.DataFrame(schema=schema)

    td = trade_df.with_columns(
        [
            pl.col("buy_id").cast(pl.Int64, strict=False).alias("buy_id"),
            pl.col("sell_id").cast(pl.Int64, strict=False).alias("sell_id"),
            pl.col("volume").cast(pl.Float64, strict=False).alias("cancel_volume"),
            time_ms_expr("trade_time").alias("cancel_time_ms"),
        ]
    )
    exec_cancel = (
        pl.col("exec_type").cast(pl.Utf8) == "Cancel" if "exec_type" in td.columns else pl.lit(False)
    )
    cancel_rows = td.filter(exec_cancel | (pl.col("buy_id") == -1) | (pl.col("sell_id") == -1))
    if cancel_rows.is_empty():
        return pl.DataFrame(schema=schema)

    cancel_rows = cancel_rows.with_columns(
        pl.when(pl.col("buy_id") == -1)
        .then(pl.col("sell_id"))
        .otherwise(pl.col("buy_id"))
        .alias("order_id")
    )
    return (
        cancel_rows.filter(pl.col("order_id") > 0)
        .select("order_id", "cancel_volume", "cancel_time_ms")
        .join(
            base_orders.select("order_id", "direction", "orig_volume", "orig_time_ms"),
            on="order_id",
            how="left",
        )
        .filter(pl.col("direction").is_not_null())
    )


def cancel_events_from_order(cancel_candidates: pl.DataFrame, base_orders: pl.DataFrame) -> pl.DataFrame:
    """从 order 表提取上交所主动撤单事件。

    上交所撤单直接出现在 order 表 status=Cancelled 的记录中。同一 order_id
    既有原始委托行，也有撤单行；这里用撤单行时间作为 cancel_time，用
    base_orders 中最早的非撤单行时间作为 orig_time。
    """

    schema = {
        "order_id": pl.Int64,
        "cancel_volume": pl.Float64,
        "cancel_time_ms": pl.Int64,
        "direction": pl.Utf8,
        "orig_volume": pl.Float64,
        "orig_time_ms": pl.Int64,
    }
    if cancel_candidates.is_empty():
        return pl.DataFrame(schema=schema)

    cancel_rows = cancel_candidates.with_columns(
        [
            pl.col("order_id").cast(pl.Int64, strict=False).alias("order_id"),
            pl.col("order_volume").cast(pl.Float64, strict=False).alias("cancel_volume"),
            pl.col("direction_norm").alias("cancel_direction"),
            pl.col("order_time_ms").alias("cancel_time_ms"),
        ]
    )
    return (
        cancel_rows.filter(pl.col("order_id").is_not_null())
        .select("order_id", "cancel_volume", "cancel_time_ms", "cancel_direction")
        .join(
            base_orders.select("order_id", "direction", "orig_volume", "orig_time_ms"),
            on="order_id",
            how="left",
        )
        .with_columns(pl.coalesce(["direction", "cancel_direction"]).alias("direction"))
        .select("order_id", "cancel_volume", "cancel_time_ms", "direction", "orig_volume", "orig_time_ms")
        .filter(pl.col("direction").is_not_null())
    )


def empty_result(stock_code: str) -> StockResult:
    """生成空结果，便于缺文件或异常股票安全跳过。"""

    return StockResult(
        stock_code=stock_code,
        volumes={},
        tox_5s_count=0,
        tox_30s_count=0,
        order_count=0,
        cancel_event_count=0,
        negative_order_count=0,
    )


def process_stock_task(task: tuple[str, str, str, str]) -> StockResult:
    """处理单只股票一天的数据。

    返回值只包含轻量聚合结果，不把大 DataFrame 传回主进程，降低并行通信开销。
    """

    stock_code, date, order_path_str, trade_path_str = task
    order_path = Path(order_path_str)
    trade_path = Path(trade_path_str)

    # 单股文件缺失时直接返回空结果。这里不抛异常，是为了全市场并行时
    # 个别股票问题不影响整个交易日继续处理。
    if not order_path.exists() or not trade_path.exists():
        return empty_result(stock_code)

    # 读入该股票当日的原始委托表和逐笔成交/撤单表。
    # order 表用于定义“订单从哪里来”，trade 表用于判断“是否成交/是否撤单”。
    order_df = read_csv_auto(order_path)
    trade_df = read_csv_auto(trade_path)
    if order_df.is_empty():
        return empty_result(stock_code)

    # base_orders：每个 order_id 的原始委托信息。
    # order_cancel_candidates：上交所 order 表中 status=Cancelled 的撤单记录。
    base_orders, order_cancel_candidates = prepare_orders(order_df)
    if base_orders.is_empty():
        return empty_result(stock_code)

    # 成交量要先独立汇总出来，后面两处都会用到：
    # 1) 主动撤单事件若已有成交量，则该撤单归为部撤；
    # 2) 没成交也没撤单的订单，才归为废单 negative。
    exchange = infer_exchange(order_path, order_df)
    trades = trade_volume_by_order(trade_df)

    # 不同交易所的主动撤单记录位置不同：
    # - SSE：撤单在 order 表 status 字段里；
    # - SZSE：撤单在 trade 表中，buy_id/sell_id 一侧为 -1。
    if exchange == "SSE":
        cancel_events = cancel_events_from_order(order_cancel_candidates, base_orders)
    elif exchange == "SZSE":
        cancel_events = cancel_events_from_trade(trade_df, base_orders)
    else:
        cancel_events = pl.concat(
            [
                cancel_events_from_order(order_cancel_candidates, base_orders),
                cancel_events_from_trade(trade_df, base_orders),
            ],
            how="vertical",
        )

    volumes: dict[str, float] = {}
    tox_5s_count = 0
    tox_30s_count = 0

    if not cancel_events.is_empty():
        # 将主动撤单事件与同一 order_id 的历史成交量合并。
        # trade_volume > 0 表示该订单先成交过一部分，剩余部分又被撤掉，即部撤。
        # trade_volume == 0 表示该订单没有成交过就被撤掉，即全撤。
        cancel_events = (
            cancel_events.join(trades, on="order_id", how="left")
            .with_columns(
                [
                    pl.col("trade_volume").fill_null(0.0),
                    segment_expr("cancel_time_ms").alias("segment"),
                    pl.when(pl.col("trade_volume").fill_null(0.0) > 0)
                    .then(pl.lit("part_cancel"))
                    .otherwise(pl.lit("all_cancel"))
                    .alias("kind"),
                    (pl.col("cancel_time_ms") - pl.col("orig_time_ms")).alias("delta_ms"),
                ]
            )
        )

        # 三小将的全撤/部撤部分按“撤单发生时间”归入配置的时间段。
        # 这里先只汇总分类订单量，后面写矩阵时再除以自由流通股本。
        grouped = (
            cancel_events.filter(
                pl.col("segment").is_not_null()
                & pl.col("direction").is_in(DIRECTIONS)
                & pl.col("cancel_volume").is_not_null()
            )
            .group_by(["direction", "kind", "segment"], maintain_order=False)
            .agg(pl.col("cancel_volume").sum().alias("volume"))
        )
        for row in grouped.iter_rows(named=True):
            key = f"{row['direction']}_{row['kind']}_rate_{row['segment']}"
            volumes[key] = float(row["volume"] or 0.0)

        # 毒流动性只关心撤单速度：撤单时间 - 原始委托时间。
        # 分子为 5 秒内主动撤单数量，分母为 30 秒内主动撤单数量。
        tox_counts = cancel_events.select(
            [
                (
                    (pl.col("delta_ms") >= 0)
                    & (pl.col("delta_ms") <= 5 * MS_PER_SECOND)
                )
                .cast(pl.Int64)
                .sum()
                .alias("tox_5"),
                (
                    (pl.col("delta_ms") >= 0)
                    & (pl.col("delta_ms") <= 30 * MS_PER_SECOND)
                )
                .cast(pl.Int64)
                .sum()
                .alias("tox_30"),
            ]
        ).row(0, named=True)
        tox_5s_count = int(tox_counts["tox_5"] or 0)
        tox_30s_count = int(tox_counts["tox_30"] or 0)

    # 为了识别废单，需要把主动撤单事件先压到 order_id 粒度。
    # 后续与 base_orders、trades 合并后：
    #   trade_volume == 0 且 cancel_volume == 0
    # 才表示“既没有成交，也没有主动撤单，最终留在簿上未成交”的废单。
    cancel_by_order = (
        cancel_events.group_by("order_id", maintain_order=False)
        .agg(pl.col("cancel_volume").sum().alias("cancel_volume"))
        if not cancel_events.is_empty()
        else pl.DataFrame({"order_id": [], "cancel_volume": []}, schema={"order_id": pl.Int64, "cancel_volume": pl.Float64})
    )

    # 订单生命周期总表：每个原始委托 order_id 一行，带出成交量、撤单量和委托时段。
    # 废单没有撤单时间，因此按原始委托时间归入时间段。
    classified_orders = (
        base_orders.join(trades, on="order_id", how="left")
        .join(cancel_by_order, on="order_id", how="left")
        .with_columns(
            [
                pl.col("trade_volume").fill_null(0.0),
                pl.col("cancel_volume").fill_null(0.0),
                segment_expr("orig_time_ms").alias("segment"),
            ]
        )
    )
    # negative 是“未成交且未主动撤单”的留存委托。它不是 trade 表里的 Cancel，
    # 也不是 order 表里的 Cancelled，而是通过排除成交和主动撤单得到。
    negative_orders = classified_orders.filter(
        (pl.col("trade_volume") == 0)
        & (pl.col("cancel_volume") == 0)
        & pl.col("direction").is_in(DIRECTIONS)
        & pl.col("segment").is_not_null()
    )
    if not negative_orders.is_empty():
        grouped_negative = negative_orders.group_by(["direction", "segment"], maintain_order=False).agg(
            pl.col("orig_volume").sum().alias("volume")
        )
        for row in grouped_negative.iter_rows(named=True):
            key = f"{row['direction']}_negative_rate_{row['segment']}"
            volumes[key] = float(row["volume"] or 0.0)

    # 返回轻量结果给主进程。主进程负责除以自由流通股本、写 npy 和刷新进度条。
    return StockResult(
        stock_code=stock_code,
        volumes=volumes,
        tox_5s_count=tox_5s_count,
        tox_30s_count=tox_30s_count,
        order_count=base_orders.height,
        cancel_event_count=cancel_events.height if not cancel_events.is_empty() else 0,
        negative_order_count=negative_orders.height,
    )


def load_axes() -> tuple[list[str], list[str], dict[str, int], dict[str, int]]:
    """读取 EOD 股票轴和日期轴，并构造写矩阵用的坐标映射。"""

    tickers_raw = np.load(config.TICKER_NAMES_NPY_PATH, allow_pickle=True)
    dates_raw = np.load(config.DATES_NPY_PATH, allow_pickle=True)
    tickers = [normalize_stock_code(x) for x in tickers_raw]
    dates = ["".join(ch for ch in _to_text(x) if ch.isdigit())[:8] for x in dates_raw]
    return (
        tickers,
        dates,
        {ticker: idx for idx, ticker in enumerate(tickers) if ticker},
        {date: idx for idx, date in enumerate(dates) if date},
    )


def validate_axes(tickers: list[str], dates: list[str]) -> None:
    """校验自由流通股本、可交易矩阵与 EOD 轴完全一致。"""

    fund_tickers_raw = np.load(config.FUNDAMENTAL_TICKER_NAMES_NPY_PATH, allow_pickle=True)
    fund_dates_raw = np.load(config.FUNDAMENTAL_DATES_NPY_PATH, allow_pickle=True)
    fund_tickers = [normalize_stock_code(x) for x in fund_tickers_raw]
    fund_dates = ["".join(ch for ch in _to_text(x) if ch.isdigit())[:8] for x in fund_dates_raw]
    if fund_tickers != tickers:
        raise ValueError("基本面股票轴与 EOD 股票轴不一致")
    if fund_dates != dates:
        raise ValueError("基本面日期轴与 EOD 日期轴不一致")

    free_float = np.load(config.FREE_FLOAT_SHARES_NPY_PATH, mmap_mode="r")
    tradable = np.load(config.TRADABLE_NPY_PATH, mmap_mode="r")
    expected = (len(tickers), len(dates))
    if free_float.shape != expected:
        raise ValueError(f"自由流通股本矩阵形状 {free_float.shape} 与 EOD 轴形状 {expected} 不一致")
    if tradable.shape != expected:
        raise ValueError(f"可交易股票矩阵形状 {tradable.shape} 与 EOD 轴形状 {expected} 不一致")


def scan_dates(date_filter: set[str] | None = None) -> list[str]:
    """扫描 Level-2 目录中可处理的交易日。"""

    if not config.LEVEL2_DATA_DIR.exists():
        raise FileNotFoundError(f"Level-2 数据目录不存在：{config.LEVEL2_DATA_DIR}")
    dates: list[str] = []
    for path in config.LEVEL2_DATA_DIR.iterdir():
        if not path.is_dir() or not path.name.isdigit() or len(path.name) != 8:
            continue
        if date_filter is not None and path.name not in date_filter:
            continue
        if config.START_DATE and path.name < config.START_DATE:
            continue
        if config.END_DATE and path.name > config.END_DATE:
            continue
        if (path / "order").exists() and (path / "trade").exists():
            dates.append(path.name)
    return sorted(dates)


def initialize_memmaps(names: list[str], shape: tuple[int, int], output_dir: Path) -> dict[str, np.memmap]:
    """初始化 raw 因子矩阵。

    npy 文件较大，使用 open_memmap 分块填 NaN，避免一次性分配全部内存。
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    arrays: dict[str, np.memmap] = {}
    logger.info(f"初始化 raw 因子文件：count={len(names)}, shape={shape}, dir={output_dir}")
    for name in names:
        arr = open_memmap(output_dir / f"{name}.npy", mode="w+", dtype=np.float64, shape=shape)
        for start in range(0, shape[0], 128):
            arr[start : start + 128, :] = np.nan
        arr.flush()
        arrays[name] = arr
    return arrays


def rolling_nanmean_fixed_window(src: np.ndarray, window: int, dst_path: Path, block_rows: int = 64) -> None:
    """沿日期轴计算固定窗口 nanmean。

    注意这里的“固定窗口”指最近 N 个交易日位置，而不是向前寻找 N 个有效值。
    窗口内有 NaN 就跳过；若整个窗口全是 NaN，则输出 NaN。
    """

    rows, cols = src.shape
    dst = open_memmap(dst_path, mode="w+", dtype=np.float64, shape=src.shape)
    end_idx = np.arange(cols, dtype=np.int64) + 1
    start_idx = np.maximum(0, end_idx - window)

    for row_start in range(0, rows, block_rows):
        # 分块处理股票维，避免一次性把完整矩阵读入内存。
        block = np.asarray(src[row_start : row_start + block_rows, :], dtype=np.float64)
        valid = np.isfinite(block)
        values = np.where(valid, block, 0.0)
        counts = valid.astype(np.int32)

        # 用前缀和做固定窗口 rolling nanmean：
        # sums = 最近 N 个日期位置的有效值之和；
        # cnts = 最近 N 个日期位置的非 NaN 个数。
        csum = np.concatenate(
            [np.zeros((block.shape[0], 1), dtype=np.float64), np.cumsum(values, axis=1)],
            axis=1,
        )
        ccnt = np.concatenate(
            [np.zeros((block.shape[0], 1), dtype=np.int32), np.cumsum(counts, axis=1)],
            axis=1,
        )
        sums = csum[:, end_idx] - csum[:, start_idx]
        cnts = ccnt[:, end_idx] - ccnt[:, start_idx]
        out = np.full(block.shape, np.nan, dtype=np.float64)
        # cnts == 0 的窗口保持 NaN；cnts > 0 时才做除法。
        np.divide(sums, cnts, out=out, where=cnts > 0)
        dst[row_start : row_start + block.shape[0], :] = out

    dst.flush()
    del dst


def build_roll_outputs(names: list[str], raw_dir: Path, output_root: Path, shape: tuple[int, int]) -> None:
    """基于 raw 因子生成 roll5 和 roll20 两套平滑结果。"""

    for window in (5, 20):
        roll_dir = output_root / f"roll{window}"
        roll_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"开始生成 roll{window}：dir={roll_dir}")
        for name in tqdm(names, desc=f"生成 roll{window}", dynamic_ncols=True):
            src = np.load(raw_dir / f"{name}.npy", mmap_mode="r")
            if src.shape != shape:
                raise ValueError(f"{name}.npy 矩阵形状 {src.shape} 与 EOD 轴形状 {shape} 不一致")
            rolling_nanmean_fixed_window(src, window, roll_dir / f"{name}.npy")


def available_tasks_for_date(
    date: str,
    ticker_to_idx: dict[str, int],
    tradable: np.ndarray,
    stock_filter: set[str] | None,
) -> list[tuple[str, str, str, str]]:
    """生成某个交易日的股票任务列表，只保留可交易且 order/trade 均存在的股票。"""

    date_dir = config.LEVEL2_DATA_DIR / date
    order_map = build_file_map(date_dir / "order")
    trade_map = build_file_map(date_dir / "trade")
    common = sorted(set(order_map) & set(trade_map) & set(ticker_to_idx))
    if stock_filter is not None:
        common = [stock for stock in common if stock in stock_filter]

    tasks: list[tuple[str, str, str, str]] = []
    for stock in common:
        idx = ticker_to_idx[stock]
        try:
            is_tradable = int(tradable[idx]) == 1
        except Exception:
            is_tradable = False
        if is_tradable:
            tasks.append((stock, date, str(order_map[stock]), str(trade_map[stock])))
    return tasks


def write_result_to_raw(
    result: StockResult,
    date_idx: int,
    ticker_to_idx: dict[str, int],
    free_float: np.ndarray,
    arrays: dict[str, np.memmap],
) -> None:
    """把单股聚合结果写入所有 raw 因子矩阵的对应 (stock, date) 位置。"""

    stock_idx = ticker_to_idx.get(result.stock_code)
    if stock_idx is None:
        return

    # 三小将分母是自由流通股本。分母缺失、非有限或 <=0 时不写该股票当日的
    # 三小将类因子，矩阵中保留初始化的 NaN。
    shares = float(free_float[stock_idx, date_idx])
    single_values: dict[str, float] = {}
    if np.isfinite(shares) and shares > 0:
        for segment in SEGMENT_NAMES:
            for direction in DIRECTIONS:
                # 单项因子：分类订单量 / 自由流通股本。
                for kind in CANCEL_KINDS:
                    name = f"{direction}_{kind}_rate_{segment}"
                    value = result.volumes.get(name, 0.0) / shares
                    arrays[name][stock_idx, date_idx] = value
                    single_values[name] = value

                # 合成三小将：同一方向下，全撤、部撤、废单三个单项因子等权平均。
                tri_name = f"{direction}_tri_{segment}"
                tri_parts = [
                    single_values[f"{direction}_all_cancel_rate_{segment}"],
                    single_values[f"{direction}_part_cancel_rate_{segment}"],
                    single_values[f"{direction}_negative_rate_{segment}"],
                ]
                arrays[tri_name][stock_idx, date_idx] = float(np.mean(tri_parts))

    # 毒流动性不使用自由流通股本做分母；只要 30 秒内撤单数量非 0 就可以写入。
    if result.tox_30s_count > 0:
        arrays["tox_5s_over_30s"][stock_idx, date_idx] = result.tox_5s_count / result.tox_30s_count


def dry_run_report(results: list[StockResult]) -> None:
    """dry-run 模式下输出样本诊断信息到 logger。"""

    for result in results:
        logger.info(
            f"{result.stock_code}: 原始委托数={result.order_count}, "
            f"主动撤单事件数={result.cancel_event_count}, 废单数={result.negative_order_count}, "
            f"毒流动性计数={result.tox_5s_count}/{result.tox_30s_count}"
        )
        shown = 0
        for key in sorted(result.volumes):
            if result.volumes[key] != 0:
                logger.info(f"  {key}: {result.volumes[key]:.4f}")
                shown += 1
                if shown >= 12:
                    remaining = sum(1 for v in result.volumes.values() if v != 0) - shown
                    if remaining > 0:
                        logger.info(f"  ... 还有 {remaining} 个非零订单量分桶未展示")
                    break


def process_date(
    date: str,
    date_idx: int,
    ticker_to_idx: dict[str, int],
    tradable_matrix: np.ndarray,
    free_float: np.ndarray,
    arrays: dict[str, np.memmap] | None,
    stock_filter: set[str] | None,
    n_jobs: int,
    dry_run: bool,
) -> list[StockResult]:
    """处理一个交易日。

    并行粒度是“股票”。主进程负责 tqdm 进度条和写 npy，worker 只负责读 CSV
    与聚合，避免多个进程同时写同一个 npy 文件。
    """

    tradable_col = tradable_matrix[:, date_idx]
    tasks = available_tasks_for_date(date, ticker_to_idx, tradable_col, stock_filter)
    if not tasks:
        logger.warning(f"{date}: 没有找到同时具备 order/trade 文件的可交易股票")
        return []

    workers = max(1, min(int(n_jobs), len(tasks)))
    logger.info(f"{date}: 股票任务数={len(tasks)}, 并行进程数={workers}")
    results: list[StockResult] = []
    if workers == 1:
        # 单进程路径主要用于调试，异常堆栈更直观。
        iterator = (process_stock_task(task) for task in tasks)
        for result in tqdm(iterator, total=len(tasks), desc=f"{date} 股票处理", dynamic_ncols=True):
            results.append(result)
            if arrays is not None:
                write_result_to_raw(result, date_idx, ticker_to_idx, free_float, arrays)
    else:
        # 多进程路径用于全市场运行。worker 只返回 StockResult，写 npy 仍在主进程串行完成，
        # 这样可以避免多个进程同时写同一批 memmap 文件。
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(process_stock_task, task): task for task in tasks}
            for future in tqdm(
                as_completed(future_map),
                total=len(future_map),
                desc=f"{date} 股票处理",
                dynamic_ncols=True,
            ):
                result = future.result()
                results.append(result)
                if arrays is not None:
                    write_result_to_raw(result, date_idx, ticker_to_idx, free_float, arrays)

    if dry_run:
        dry_run_report(results)
    return results


def parse_csv_arg(value: str | None) -> set[str] | None:
    """解析逗号分隔的日期或股票过滤参数。"""

    if value is None or not value.strip():
        return None
    return {normalize_stock_code(item) if not item.strip().isdigit() or len(item.strip()) != 8 else item.strip() for item in value.split(",") if item.strip()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="复现研报撤单因子并输出 EOD 形状 npy。")
    parser.add_argument("--dates", help="逗号分隔的交易日，例如 20240226,20200730。")
    parser.add_argument("--stocks", help="逗号分隔的股票代码，用于样本检查或小范围运行。")
    parser.add_argument("--n-jobs", type=int, default=config.N_JOBS)
    parser.add_argument("--dry-run", action="store_true", help="只读取和聚合样本，不写出 npy。")
    parser.add_argument("--skip-roll", action="store_true", help="只写 raw 因子，不生成 roll5/roll20。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    requested_dates = parse_csv_arg(args.dates)
    requested_stocks = parse_csv_arg(args.stocks)
    if requested_stocks is not None:
        requested_stocks = {normalize_stock_code(stock) for stock in requested_stocks}

    tickers, dates, ticker_to_idx, date_to_idx = load_axes()
    validate_axes(tickers, dates)
    shape = (len(tickers), len(dates))
    names = factor_names()
    logger.info(
        f"启动撤单因子复现：shape={shape}, 因子数={len(names)}, "
        f"时间段={','.join(SEGMENT_NAMES)}, dry_run={args.dry_run}"
    )

    process_dates = [date for date in scan_dates(requested_dates) if date in date_to_idx]
    if not process_dates:
        raise ValueError("没有 Level-2 交易日同时满足筛选条件和 EOD 日期轴")
    logger.info(f"待处理交易日数={len(process_dates)}: {process_dates[:10]}")

    tradable = np.load(config.TRADABLE_NPY_PATH, mmap_mode="r")
    free_float = np.load(config.FREE_FLOAT_SHARES_NPY_PATH, mmap_mode="r")

    arrays = None
    raw_dir = config.REPORT_FACTOR_OUTPUT_DIR / "raw"
    if not args.dry_run:
        arrays = initialize_memmaps(names, shape, raw_dir)

    try:
        for date in process_dates:
            logger.info(f"开始处理交易日 {date}")
            process_date(
                date=date,
                date_idx=date_to_idx[date],
                ticker_to_idx=ticker_to_idx,
                tradable_matrix=tradable,
                free_float=free_float,
                arrays=arrays,
                stock_filter=requested_stocks,
                n_jobs=args.n_jobs,
                dry_run=args.dry_run,
            )
            logger.info(f"完成处理交易日 {date}")
    finally:
        if arrays is not None:
            for arr in arrays.values():
                arr.flush()
            del arrays

    if not args.dry_run and not args.skip_roll:
        build_roll_outputs(names, raw_dir, config.REPORT_FACTOR_OUTPUT_DIR, shape)

    logger.info("撤单因子复现任务完成")


if __name__ == "__main__":
    main()
