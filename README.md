# 双来源时间带联接的水位与迟到旁路

本项目保存虚构实验设施的仓湖源数据与Python读取工具。现有工具只加载文件并显示条数，不执行事实加工、解码、合并、联接或重映射。全部设备、样品、记录和数值均为合成。

## 字段与单位

stream按source_offset读取，kind是record或watermark，side取L/R。水位保证未来正常接受时间不小于W，晚到走旁路；此处只有合成消息。

material-index.json记录全部输入内容哈希；fixtures保存原始资料与配置，计算输出可放.workspace-output，原始资料不应被覆盖。

## 运行已有读取器

Python 3.11以上，标准库即可，不需安装第三方依赖。

    python -m src.band_reader fixtures/stream.jsonl
    python -m compileall -q src tests
    python -m unittest discover -s tests -v

## 有界事件时间联接组件

src/event_time_join.py 在本地进程内实现双源事件时间联接，不启动真实流处理集群：

- 匹配：同 device 且 -early <= r.time - l.time <= late（闭区间），输出全部符合行对而非最近一个；
- record_id 每侧不可变：同内容重送幂等（不产生新输出），冲突内容拒绝并计入 reject 事件；
- 水位只来自显式 watermark 消息，每侧单调不减（回退拒绝），不从已见最大时间臆造；
- time < W 的新到记录进 late 旁路，记录被违反的水位与可能影响范围（含已封存记录），不回改已发布行对；time == W 仍正常；
- 释放严格大于：左记录需 W_R > time + late，右记录需 W_L > time + early，等于仍保留等待；release 事件带水位依据；
- 等待状态内存上限 buffer_max_records，超出溢写 state/spill.jsonl，检查点是权威快照；
- 每个源批先提交输出再原子写检查点（last_offset + output_committed + 状态快照），恢复时截断未提交输出，不漏不重。

    python -m src.join_runner --stream fixtures/stream.jsonl --band fixtures/band.json --workdir .workspace-output/join-run
    python -m src.join_runner --stream fixtures/stream.jsonl --band fixtures/band.json --workdir .workspace-output/join-run --stop-after 5  # 模拟故障：当前批不提交
    python -m src.join_runner --stream fixtures/stream.jsonl --band fixtures/band.json --workdir .workspace-output/join-run --resume       # 从最新检查点恢复续跑

output.jsonl 含 pair / late / release / reject 四类事件；checkpoints/ 保存每批一致性快照；summary.json 汇总计数、水位与未释放记录的等待依据。


## 固定容器

    docker build -t watermarked-band-join:local .
    docker run --rm watermarked-band-join:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
