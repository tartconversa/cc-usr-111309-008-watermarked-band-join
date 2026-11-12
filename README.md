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

## 固定容器

    docker build -t watermarked-band-join:local .
    docker run --rm watermarked-band-join:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
