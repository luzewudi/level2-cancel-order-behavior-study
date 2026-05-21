from __future__ import annotations

"""
step2_因子计算.py

基于 step1 输出的“日频全量撤单基础数据”：
  因子缓存/全量/撤单全量数据.pkl

计算 10 个撤单占比因子（笔数/撤单量各 5 个）并对齐到原始数据的：
  - dates.npy        （日期轴）
  - ticker_names.npy （股票轴）

输出：
  因子缓存/因子数据/*.npy
每个 npy 形状为 (n_stocks, n_dates)，空值填充为 NaN。
"""

from typing import Any

import numpy as np
import pandas as pd

import config
from utils.log_kit import get_logger

logger = get_logger()

# ========== step1 输出路径 ==========
BASE_DIR = config.BASE_DIR
CACHE_DIR = BASE_DIR / "因子缓存"
FULL_PKL_PATH = CACHE_DIR / "全量" / "撤单全量数据.pkl"
FACTOR_OUT_DIR = CACHE_DIR / "因子数据"


def _to_text(x: Any) -> str:
    """稳健地把 bytes / numpy.bytes_ 等转换为 str。"""
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
        pass
    return str(x)


def normalize_stock_code(code: Any) -> str:
    """
    将股票代码标准化为 6 位字符串。
    - 兼容 b'000001' / '000001.SZ' / 'SZ000001' 等
    """
    s = _to_text(code).strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    if digits:
        return digits.zfill(6)
    return s


def _load_tickers_and_dates() -> tuple[list[str], list[str]]:
    """读取 ticker_names.npy 与 dates.npy，并统一为（6位ticker、8位YYYYMMDD日期）。"""
    if not config.TICKER_NAMES_NPY_PATH.exists():
        raise FileNotFoundError(f"未找到 ticker_names.npy：{config.TICKER_NAMES_NPY_PATH}")
    if not config.DATES_NPY_PATH.exists():
        raise FileNotFoundError(f"未找到 dates.npy：{config.DATES_NPY_PATH}")

    ticker_names_raw = np.load(config.TICKER_NAMES_NPY_PATH, allow_pickle=True)
    dates_raw = np.load(config.DATES_NPY_PATH, allow_pickle=True)

    tickers: list[str] = [normalize_stock_code(t) for t in ticker_names_raw]

    dates_8: list[str] = []
    for d in dates_raw:
        s = _to_text(d).strip()
        if "-" in s:
            s = s.replace("-", "")
        s = "".join(ch for ch in s if ch.isdigit())[:8]
        dates_8.append(s)

    return tickers, dates_8


def _safe_div(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
    """安全除法：denom==0 时返回 NaN。"""
    numer = numer.astype("float64", copy=False)
    denom = denom.astype("float64", copy=False)
    out = np.full_like(numer, np.nan, dtype="float64")
    np.divide(numer, denom, out=out, where=(denom != 0))
    return out


def main() -> None:
    print("=" * 60)
    print("step2_因子计算：从撤单全量数据生成 10 个因子矩阵（npy）")
    print("=" * 60)

    tickers, dates_8 = _load_tickers_and_dates()
    n_stocks = len(tickers)
    n_dates = len(dates_8)
    if n_stocks == 0 or n_dates == 0:
        raise ValueError(f"ticker_names 或 dates 为空：stocks={n_stocks}, dates={n_dates}")

    ticker_to_idx = {}
    for i, t in enumerate(tickers):
        if t and t not in ticker_to_idx:
            ticker_to_idx[t] = i

    date_to_idx = {}
    for j, d8 in enumerate(dates_8):
        if d8 and d8 not in date_to_idx:
            date_to_idx[d8] = j

    if not FULL_PKL_PATH.exists():
        raise FileNotFoundError(f"未找到 step1 输出全量 pkl：{FULL_PKL_PATH}")

    logger.info(f"读取全量撤单数据：{FULL_PKL_PATH}")
    df = pd.read_pickle(FULL_PKL_PATH)
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError(f"全量 pkl 为空或不是 DataFrame：{FULL_PKL_PATH}")

    required_cols = [
        "stock_code",
        "date",
        "cancel_buy_count",
        "cancel_sell_count",
        "cancel_buy_volume",
        "cancel_sell_volume",
        "algo_cancel_buy_count",
        "algo_cancel_sell_count",
        "algo_cancel_buy_volume",
        "algo_cancel_sell_volume",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"全量 pkl 缺少必要列：{missing}；请确认 step1 输出字段")

    # 统一 stock_code（尽量走向量化：抽取数字并取最后 6 位）
    stock_raw = df["stock_code"].astype(str)
    digits = stock_raw.str.replace(r"\D", "", regex=True)
    stock6 = digits.str[-6:]
    stock6 = stock6.where(stock6.notna(), "")
    # 长度不足 6 的做 zfill（极少数异常）
    need_zfill = stock6.str.len().fillna(0).astype(int) < 6
    if need_zfill.any():
        stock6.loc[need_zfill] = stock6.loc[need_zfill].str.zfill(6)
    df["_ticker6"] = stock6

    # 统一 date -> 8位字符串 YYYYMMDD
    dt = pd.to_datetime(df["date"], errors="coerce")
    df["_date8"] = dt.dt.strftime("%Y%m%d")

    # 映射到矩阵坐标
    df["_i"] = df["_ticker6"].map(ticker_to_idx)
    df["_j"] = df["_date8"].map(date_to_idx)
    valid_mask = df["_i"].notna() & df["_j"].notna()
    df_valid = df.loc[valid_mask].copy()

    # 若存在重复 (stock, date)，保留最后一条（step1 理论上不会重复）
    df_valid.sort_values(["_i", "_j"], inplace=True, kind="mergesort")
    df_valid.drop_duplicates(["_i", "_j"], keep="last", inplace=True)

    i_idx = df_valid["_i"].astype(int).to_numpy()
    j_idx = df_valid["_j"].astype(int).to_numpy()

    # 数值列转 numpy
    cb_cnt = df_valid["cancel_buy_count"].to_numpy()
    cs_cnt = df_valid["cancel_sell_count"].to_numpy()
    cab_cnt = df_valid["algo_cancel_buy_count"].to_numpy()
    cas_cnt = df_valid["algo_cancel_sell_count"].to_numpy()

    cb_vol = df_valid["cancel_buy_volume"].to_numpy()
    cs_vol = df_valid["cancel_sell_volume"].to_numpy()
    cab_vol = df_valid["algo_cancel_buy_volume"].to_numpy()
    cas_vol = df_valid["algo_cancel_sell_volume"].to_numpy()

    all_cnt = cb_cnt + cs_cnt
    all_algo_cnt = cab_cnt + cas_cnt

    all_vol = cb_vol + cs_vol
    all_algo_vol = cab_vol + cas_vol

    # 10 个因子（值向量）
    factor_values: dict[str, np.ndarray] = {
        # ====== 笔数口径 ======
        "buy_algo_cnt_over_all_cnt": _safe_div(cab_cnt, all_cnt),
        "sell_algo_cnt_over_all_cnt": _safe_div(cas_cnt, all_cnt),
        "all_algo_cnt_over_all_cnt": _safe_div(all_algo_cnt, all_cnt),
        "buy_algo_cnt_over_buy_cnt": _safe_div(cab_cnt, cb_cnt),
        "sell_algo_cnt_over_sell_cnt": _safe_div(cas_cnt, cs_cnt),
        # ====== 撤单量口径 ======
        "buy_algo_vol_over_all_vol": _safe_div(cab_vol, all_vol),
        "sell_algo_vol_over_all_vol": _safe_div(cas_vol, all_vol),
        "all_algo_vol_over_all_vol": _safe_div(all_algo_vol, all_vol),
        "buy_algo_vol_over_buy_vol": _safe_div(cab_vol, cb_vol),
        "sell_algo_vol_over_sell_vol": _safe_div(cas_vol, cs_vol),
    }

    FACTOR_OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 生成并写出 10 个矩阵
    written = 0
    for name, vals in factor_values.items():
        # 使用 float64 存储，以保证与原先预期的文件大小一致（每个元素 8 字节）
        mat = np.full((n_stocks, n_dates), np.nan, dtype=np.float64)
        mat[i_idx, j_idx] = vals.astype(np.float64, copy=False)
        out_path = FACTOR_OUT_DIR / f"{name}.npy"
        np.save(out_path, mat)
        written += 1
        logger.info(f"写出因子：{out_path} shape={mat.shape}")

    # 简单统计提示
    n_total = len(df)
    n_valid = len(df_valid)
    print(f"全量行数={n_total}，成功映射到 (ticker,date) 并落盘={n_valid}")
    print(f"已写出 {written} 个因子到：{FACTOR_OUT_DIR}")


if __name__ == "__main__":
    main()

