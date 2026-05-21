# level2-cancel-order-behavior-study

对于《20240124-开源证券-市场微观结构研究系列（22）：订单流系列，撤单行为规律初探》的复现与扩展。

## 项目结构

- `build_report_factors.py`: 复现买卖方向三小将因子和毒流动性因子的主入口
- `config.py`: 数据路径、输出路径、时间段和并行参数配置
- `20240124-开源证券-市场微观结构研究系列（22）：订单流系列，撤单行为规律初探.pdf`: 参考报告
- `utils/`: 路径、日志、绘图和配色工具

## 使用方式

默认只跑研报明确使用的 `09:15-09:20` 段。若要跑七段，在 `config.py` 中改为：

```python
TIME_SEGMENTS = ALL_7_TIME_SEGMENTS
```

样本检查，不写出 `.npy`：

```bash
python build_report_factors.py --dates 20240226 --stocks 000001,600000 --dry-run --n-jobs 2
```

全量生成 raw、5日均值、20日均值：

```bash
python build_report_factors.py
```

输出目录为 `factor_outputs/report_reproduction/`。生成的 `.npy` 文件与 EOD 的 `ticker_names.npy`、`dates.npy` 轴完全一致。运行日志写入当前项目目录下的 `logs/build_report_factors.log`。

## 路径配置

`config.py` 会按操作系统自动切换数据路径。

Windows 本地默认路径：

```text
Level-2: D:\凯纳\原始数据\level2_data
EOD: D:\凯纳\原始数据\eod
基本面: D:\凯纳\原始数据\fundmental
可交易股票: D:\凯纳\原始数据\eod\可交易股票.npy
```

Linux 服务器默认路径：

```text
Level-2: /mnt/ssd/fundmental
EOD: /mnt/ssd/fundmental
基本面: /mnt/ssd/eod
可交易股票: /home/luze/可交易股票.npy
```

## build_report_factors.py 流程

### 1. 读取配置与轴信息

脚本首先读取 `config.py` 中的路径和参数：

- Level-2 原始数据目录：`LEVEL2_DATA_DIR`
- EOD 轴文件：`ticker_names.npy`、`dates.npy`
- 可交易股票矩阵：`可交易股票.npy`
- 自由流通股本：`CAPQ0_FLOAT_A_SHR.npy`
- 输出目录：`factor_outputs/report_reproduction/`
- 时间段配置：默认 `09:15-09:20`

随后读取 EOD 股票轴和日期轴，构造：

- `ticker_to_idx`: 股票代码到矩阵行号
- `date_to_idx`: 交易日到矩阵列号

所有输出因子都写成 `(stock, date)` 矩阵，形状与 EOD 完全一致。

### 2. 校验基础数据

正式计算前会校验：

- 自由流通股本的股票轴、日期轴与 EOD 一致
- `CAPQ0_FLOAT_A_SHR.npy` 形状与 EOD 一致
- `可交易股票.npy` 形状与 EOD 一致

如果轴不一致，脚本会直接报错，避免输出错位因子。

### 3. 扫描交易日与股票任务

脚本扫描 `LEVEL2_DATA_DIR/YYYYMMDD/` 下同时存在 `order/` 和 `trade/` 的交易日。

每个交易日内，会分别扫描：

- `order/*.csv.gz`
- `trade/*.csv.gz`

然后只保留满足以下条件的股票：

- 股票代码能映射到 EOD 股票轴
- 当日 `可交易股票.npy` 标记为 1
- 同时存在 order 文件和 trade 文件
- 若命令行指定 `--stocks`，还要在指定股票列表内

并行计算的粒度是“单日单股”。

### 4. 单股订单生命周期重建

每个 worker 处理一只股票一天的数据，入口是 `process_stock_task()`。

它会读取：

- order 表：原始委托
- trade 表：逐笔成交和深交所撤单

order 表会被整理成 `base_orders`，即每个 `order_id` 一行的原始委托信息：

- 买卖方向
- 原始委托量
- 原始委托价格
- 原始委托时间

时间字段兼容两种格式：

- 8 位 `HHMMSSmmm`，例如 `91500120` 表示 `09:15:00.120`
- 9 位 `HHMMSSmmm`，例如 `130000000` 表示 `13:00:00.000`

如果遇到 UTC 纳秒时间戳，也会转成北京时间当日时间。

### 5. 汇总成交量

`trade_volume_by_order()` 从 trade 表里取真实成交记录：

- 优先使用 `exec_type == "Trade"`
- 如果没有 `exec_type`，则用 `buy_id > 0 且 sell_id > 0` 判断成交

一笔成交同时涉及买卖两边订单，所以会把：

- `buy_id`
- `sell_id`

都展开成统一的 `order_id`，再按 `order_id` 汇总 `volume`，得到：

- `trade_volume`

这个字段用于判断订单是否发生过成交。

### 6. 提取主动撤单

主动撤单的提取方式按交易所区分。

上交所 SSE：

- 撤单记录出现在 order 表中
- 通过 `status in {"Cancelled", "Cancel", "8", "4"}` 识别
- 撤单时间使用撤单记录自身的 `order_time`
- 原始委托时间使用同一 `order_id` 最早的非撤单 order 记录

深交所 SZSE：

- 撤单记录出现在 trade 表中
- 通过 `exec_type == "Cancel"` 或 `buy_id == -1 / sell_id == -1` 识别
- 非 `-1` 的一侧是真正被撤的 `order_id`
- 再按 `order_id` 匹配 `base_orders`，带出原始委托方向和原始委托时间

提取出的主动撤单事件包含：

- `order_id`
- `cancel_volume`
- `cancel_time`
- `direction`
- `orig_time`

### 7. 区分全撤、部撤、废单

主动撤单事件会再与 `trade_volume` 按 `order_id` 匹配。

分类规则：

- 全撤：`cancel_volume > 0 且 trade_volume == 0`
- 部撤：`cancel_volume > 0 且 trade_volume > 0`
- 废单：`cancel_volume == 0 且 trade_volume == 0`

其中废单不是来自某条撤单记录，而是通过订单生命周期排除得到：

1. 从原始委托 `base_orders` 出发
2. 匹配成交量 `trade_volume`
3. 匹配主动撤单量 `cancel_volume`
4. 同时没有成交也没有主动撤单的订单，视为最终留存未成交的废单

### 8. 归属时间段

全撤和部撤使用撤单发生时间 `cancel_time` 归属时间段。

废单没有撤单时间，因此使用原始委托时间 `orig_time` 归属时间段。

时间段由 `config.TIME_SEGMENTS` 控制。默认只有：

- `auction1_0915_0920`

如需七段，可改成 `ALL_7_TIME_SEGMENTS`。

### 9. 计算三小将因子

脚本同时计算买入方向和卖出方向。

每个时间段会生成 6 个原始因子：

- `buy_all_cancel_rate_{segment}`
- `buy_part_cancel_rate_{segment}`
- `buy_negative_rate_{segment}`
- `sell_all_cancel_rate_{segment}`
- `sell_part_cancel_rate_{segment}`
- `sell_negative_rate_{segment}`

这些因子的分子是对应类别的委托量，分母是自由流通股本：

```text
分类撤单率 = 分类委托量 / CAPQ0_FLOAT_A_SHR
```

每个时间段还会生成 2 个合成因子：

- `buy_tri_{segment}`
- `sell_tri_{segment}`

合成方式是三类原始因子等权平均：

```text
buy_tri = mean(buy_all_cancel_rate, buy_part_cancel_rate, buy_negative_rate)
sell_tri = mean(sell_all_cancel_rate, sell_part_cancel_rate, sell_negative_rate)
```

### 10. 计算毒流动性因子

毒流动性使用主动撤单事件计算，不区分买卖方向。

对每条主动撤单事件计算：

```text
delta = cancel_time - orig_time
```

然后统计：

```text
tox_5s_over_30s = 5秒内撤单数量 / 30秒内撤单数量
```

如果 30 秒内撤单数量为 0，则该股票当日输出 `NaN`。

### 11. 写出 raw 因子矩阵

主进程负责写 `.npy`，worker 不直接写文件。

这样设计是为了避免多个进程同时写同一个 memmap 文件。

每个 raw 因子都是一个 `float64` 矩阵：

```text
shape = (len(ticker_names), len(dates))
```

不可计算位置保持 `NaN`，包括：

- 非可交易股票
- 没有 Level-2 数据的股票日期
- 分母缺失或小于等于 0 的三小将因子
- 30 秒内撤单数量为 0 的毒流动性

### 12. 生成 roll5 和 roll20

raw 因子写完后，脚本会生成：

- `roll5/`
- `roll20/`

滚动均值沿日期轴计算，规则是固定最近 N 个交易日窗口：

- `roll5`: 最近 5 个日期位置
- `roll20`: 最近 20 个日期位置

窗口内跳过 `NaN`，但不会向前寻找凑满 N 个有效值。

如果整个窗口都是 `NaN`，输出仍为 `NaN`。

## 输出文件

默认研报段会输出 9 个 raw 文件：

- 8 个买卖三小将相关因子
- 1 个毒流动性因子

如果启用七段，则会输出 57 个 raw 文件：

- `7 段 × 8 个三小将相关因子 = 56`
- `tox_5s_over_30s = 1`

每个 raw 文件都会有对应的 roll5 和 roll20 文件。

目录结构：

```text
factor_outputs/report_reproduction/
  raw/
  roll5/
  roll20/
```

## 本地数据

原始行情数据、日频 numpy 数据、因子输出、日志和临时目录默认不纳入 Git 管理。
