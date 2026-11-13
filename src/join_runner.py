"""命令行入口：用显式水位跑有界事件时间联接，输出提交到 workdir。

    python -m src.join_runner --stream fixtures/stream.jsonl --band fixtures/band.json \
        --workdir .workspace-output/join-run
    python -m src.join_runner --stream fixtures/stream.jsonl --band fixtures/band.json \
        --workdir .workspace-output/join-run --stop-after 5   # 模拟故障：当前批不提交
    python -m src.join_runner --stream fixtures/stream.jsonl --band fixtures/band.json \
        --workdir .workspace-output/join-run --resume         # 从最新检查点恢复续跑
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.band_reader import load
from src.event_time_join import CheckpointedRunner, JoinConfig


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="有界事件时间联接（显式水位、迟到旁路、检查点恢复）")
    parser.add_argument("--stream", required=True, help="合成双流消息（jsonl）")
    parser.add_argument("--band", required=True, help="时间带与水位控制配置（json）")
    parser.add_argument("--workdir", default=".workspace-output/join-run", help="输出与检查点目录")
    parser.add_argument("--resume", action="store_true", help="从最新检查点恢复并截断未提交输出")
    parser.add_argument(
        "--stop-after",
        type=int,
        default=None,
        help="处理到该 source_offset 后模拟故障停止（当前批不提交）",
    )
    args = parser.parse_args(argv)

    band = load(args.band)
    config = JoinConfig.from_band(band)
    messages = load(args.stream)
    checkpoint_offsets = band.get("checkpoint_offsets", [])

    if args.resume:
        runner = CheckpointedRunner.recover(config, args.workdir)
    else:
        runner = CheckpointedRunner(config, args.workdir)
    summary = runner.run(messages, checkpoint_offsets, stop_after_offset=args.stop_after)

    Path(args.workdir).mkdir(parents=True, exist_ok=True)
    summary_path = Path(args.workdir) / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
