from __future__ import annotations

"""
全局配置文件

- 路径相关：BASE_DIR / LEVEL2_DATA_DIR / 预处理输出目录等均在各脚本内部基于 BASE_DIR 推导
- 计算相关：并行进程数、日期范围
- 研究相关：算法撤单识别的时间窗参数（以毫秒为单位）

注意：
- 本文件尽量保持“纯配置”，不要引入第三方依赖，避免在多进程 fork/spawn 时出现副作用。
"""

from pathlib import Path

# =========================================
# 路径配置
# =========================================

# 项目根目录：默认取当前文件所在目录
BASE_DIR: Path = Path(__file__).resolve().parent

# Level-2 原始数据目录（按需修改为你本地实际路径）
# 目录结构假设为：
#   LEVEL2_DATA_DIR /
#       20240226 /
#           tick / 000001.csv.gz / ...
#           order / ...
#           trade / ...
LEVEL2_DATA_DIR: Path = BASE_DIR / "level2_data"

# EOD 日频 numpy 路径（可统一在此修改为你本地实际路径）
TRADABLE_NPY_PATH: Path = Path(r"D:\凯纳\原始数据\eod\可交易股票.npy")
TICKER_NAMES_NPY_PATH: Path = Path(r"D:\凯纳\原始数据\eod\ticker_names.npy")
DATES_NPY_PATH: Path = Path(r"D:\凯纳\原始数据\eod\dates.npy")


# =========================================
# 计算资源配置
# =========================================

# 多进程并行度：请按自己机器配置，直接修改为合适的整数即可
# 例如：8 核 CPU 可以先从 N_JOBS = 4 或 6 开始尝试
N_JOBS: int = 16


# =========================================
# 日期范围配置（可选）
# =========================================

# 统一使用 YYYYMMDD 字符串；为 None 表示不限制
START_DATE: str | None = None
END_DATE: str | None = None


# =========================================
# 算法撤单识别参数
# - 时间全部以“纳秒时间戳”为基础，但为了直观，这里配置仍以“秒 / 毫秒”为单位
# =========================================
"""
用于识别“疑似算法撤单”的时间脉冲：
- algo_time_window_ms: 单笔委托后向前看的最大时间窗口（毫秒）
- algo_time_step_s: 步长（秒），例如 3 表示在窗口内生成 3,6,9,... 这些时间点
- ALGO_TIME_POINTS_S: 额外自定义的离散时间点（秒）
- ALGO_PULSE_CENTERS_MS: 上述时间点统一转换为毫秒后的集合（内部使用）
- ALGO_PULSE_HALF_WIDTH_MS: 每个脉冲窗口的半宽度（毫秒）
"""

# 单位：毫秒 —— 单笔委托后“向前看的时间窗口长度”，默认 1 分钟
algo_time_window_ms: int = 60_000

# 单位：秒 —— 自动步长；例如配置为 3，表示要 3,6,9,12,... 直到窗口上限的所有 3 秒倍数（不包含 0）
# 如果不想用“步长自动生成”，就设为 0 或 None
algo_time_step_s: float | None = 3

# 单位：秒 —— 你自定义的相对时间点（可以看作“倍数”*1 秒），
# 例如：想要 1、3、5、6、9 秒，就直接写成下面这样：
ALGO_TIME_POINTS_S: list[float] = [1, 3, 5, 6, 9]

# 自动转换为毫秒（代码内部使用）
_window_s = algo_time_window_ms / 1000.0
_step_points_s: set[float] = set()
if algo_time_step_s is not None and float(algo_time_step_s) > 0:
    _step = float(algo_time_step_s)
    _max_k = int(_window_s // _step)
    for k in range(1, _max_k + 1):
        _step_points_s.add(k * _step)

_all_points_s = set(ALGO_TIME_POINTS_S) | _step_points_s
_points_ms = {int(float(t) * 1000) for t in _all_points_s}
ALGO_PULSE_CENTERS_MS: list[int] = sorted(_points_ms)

# 脉冲窗口半宽，默认 ±20ms
ALGO_PULSE_HALF_WIDTH_MS: int = 20


__all__ = [
    "BASE_DIR",
    "LEVEL2_DATA_DIR",
    "TRADABLE_NPY_PATH",
    "TICKER_NAMES_NPY_PATH",
    "DATES_NPY_PATH",
    "N_JOBS",
    "START_DATE",
    "END_DATE",
    "algo_time_window_ms",
    "algo_time_step_s",
    "ALGO_TIME_POINTS_S",
    "ALGO_PULSE_CENTERS_MS",
    "ALGO_PULSE_HALF_WIDTH_MS",
]

