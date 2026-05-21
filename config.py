from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent

# =========================================
# 输入数据路径
# =========================================

# Level-2 原始逐笔数据目录，目录结构约定为：
#   LEVEL2_DATA_DIR / YYYYMMDD / order/*.csv.gz
#   LEVEL2_DATA_DIR / YYYYMMDD / trade/*.csv.gz
LEVEL2_DATA_DIR = Path(r"D:\凯纳\原始数据\level2_data")

# EOD 轴文件。所有输出因子 npy 都严格复用这里的股票轴和日期轴：
#   shape = (len(ticker_names), len(dates))
EOD_DIR = Path(r"D:\凯纳\原始数据\eod")
TICKER_NAMES_NPY_PATH = EOD_DIR / "ticker_names.npy"
DATES_NPY_PATH = EOD_DIR / "dates.npy"
TRADABLE_NPY_PATH = EOD_DIR / "可交易股票.npy"

# 三小将“撤单率”的分母：自由流通股本（股数口径）。
# 该文件的股票轴和日期轴需要与 EOD 完全一致。
FREE_FLOAT_SHARES_NPY_PATH = Path(r"D:\凯纳\原始数据\fundmental\CAPQ0_FLOAT_A_SHR.npy")
FUNDAMENTAL_TICKER_NAMES_NPY_PATH = Path(r"D:\凯纳\原始数据\fundmental\ticker_names.npy")
FUNDAMENTAL_DATES_NPY_PATH = Path(r"D:\凯纳\原始数据\fundmental\dates.npy")

# =========================================
# 输出路径
# =========================================
REPORT_FACTOR_OUTPUT_DIR = PROJECT_ROOT / "factor_outputs" / "report_reproduction"

# =========================================
# 计算范围
# =========================================

# 可选日期范围，统一使用 YYYYMMDD；None 表示不限制。
START_DATE: str | None = None
END_DATE: str | None = None

# 并行进程数。Level-2 CSV 解压和解析比较吃 CPU/IO，Windows 上建议不要开太满。
N_JOBS = 8


# =========================================
# 时间段配置
# =========================================

# 研报第 3.1 节明确使用的早盘集合竞价可撤单窗口。
REPORT_AUCTION_SEGMENT = [
    ("auction1_0915_0920", "09:15:00.000", "09:20:00.000", False),
]

# 研报图 13 使用的七段交易时段。需要全七段时，把 TIME_SEGMENTS 改成：
#   TIME_SEGMENTS = ALL_7_TIME_SEGMENTS
ALL_7_TIME_SEGMENTS = [
    ("auction1_0915_0920", "09:15:00.000", "09:20:00.000", False),
    ("auction2_0920_0925", "09:20:00.000", "09:25:00.000", False),
    ("cont1_0930_1030", "09:30:00.000", "10:30:00.000", False),
    ("cont2_1030_1130", "10:30:00.000", "11:30:00.000", False),
    ("cont3_1300_1400", "13:00:00.000", "14:00:00.000", False),
    ("cont4_1400_1457", "14:00:00.000", "14:57:00.000", False),
    ("auction3_1457_1500", "14:57:00.000", "15:00:00.000", True),
]

# 默认只跑研报“三小将”使用的 09:15-09:20。
# 如果你要一次性跑七段，改为：TIME_SEGMENTS = ALL_7_TIME_SEGMENTS
TIME_SEGMENTS = REPORT_AUCTION_SEGMENT


__all__ = [
    "PROJECT_ROOT",
    "LEVEL2_DATA_DIR",
    "EOD_DIR",
    "TICKER_NAMES_NPY_PATH",
    "DATES_NPY_PATH",
    "TRADABLE_NPY_PATH",
    "FREE_FLOAT_SHARES_NPY_PATH",
    "FUNDAMENTAL_TICKER_NAMES_NPY_PATH",
    "FUNDAMENTAL_DATES_NPY_PATH",
    "REPORT_FACTOR_OUTPUT_DIR",
    "START_DATE",
    "END_DATE",
    "N_JOBS",
    "REPORT_AUCTION_SEGMENT",
    "ALL_7_TIME_SEGMENTS",
    "TIME_SEGMENTS",
]
