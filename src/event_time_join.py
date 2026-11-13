"""有界事件时间联接组件：显式水位、迟到旁路、状态释放与检查点恢复。

语义契约（与 fixtures/band.json 对齐）：

- 左记录 l 与右记录 r 匹配：device 相同且 -early <= r.time - l.time <= late，
  边界包含；输出全部符合行对，而非最近一个。
- 每侧 record_id 不可变：相同内容重送幂等（不产生新输出），冲突内容拒绝。
- 水位 W 承诺未来正常接受的记录 time >= W；time < W 的新到记录进入 late 旁路，
  不改写已封存的联接结果；time == W 仍属正常。
- 水位每侧单调不减，回退拒绝；水位只来自显式控制消息，不从已见最大时间臆造。
- 释放条件为严格大于：左 l 需 W_R > l.time + late；右 r 需 W_L > r.time + early；
  等于时仍可能到来边界匹配，不得释放。
- 每个源批的输入位置、状态与输出提交一致：检查点记录 last_offset 与
  output_committed，恢复时截断未提交输出，不漏不重。

组件只依赖标准库，不启动真实流处理集群。
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

LEFT = "L"
RIGHT = "R"
SIDES = (LEFT, RIGHT)
_MARGIN_NAME = {LEFT: "late", RIGHT: "early"}


def other_side(side: str) -> str:
    if side == LEFT:
        return RIGHT
    if side == RIGHT:
        return LEFT
    raise ValueError(f"未知侧: {side!r}")


def record_digest(message: dict) -> str:
    """record 内容的稳定摘要，用于重送幂等与冲突检测。"""
    payload = {k: message[k] for k in ("record_id", "device", "time")}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class JoinConfig:
    early: int
    late: int
    buffer_max_records: int = 1024

    @classmethod
    def from_band(cls, band: dict) -> "JoinConfig":
        if band.get("band_closed") is not True:
            raise ValueError("仅支持闭区间时间带（band_closed=true）")
        if band.get("watermark_future_time") != "greater_or_equal":
            raise ValueError("仅支持 watermark_future_time=greater_or_equal")
        if band.get("late_policy") != "side_output_no_rewrite":
            raise ValueError("仅支持 late_policy=side_output_no_rewrite")
        return cls(
            early=int(band["early"]),
            late=int(band["late"]),
            buffer_max_records=int(band.get("buffer_max_records", 1024)),
        )

    def margin(self, side: str) -> int:
        """该侧记录释放前必须等待的余地：左侧等 late，右侧等 early。"""
        return self.late if side == LEFT else self.early


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class WaitingStore:
    """匹配等待状态：内存至多 cap 条，其余溢写 state_dir/spill.jsonl。

    检查点是状态的权威快照；溢写文件只是两次检查点之间的内存减压，
    恢复时由快照重建，因此无需独立的崩溃一致性。
    """

    SPILL_NAME = "spill.jsonl"

    def __init__(self, cap: int, state_dir, margin):
        self._cap = max(1, int(cap))
        self._margin = margin
        self._dir = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._mem: dict[tuple[str, str], dict] = {}
        self._spilled: dict[tuple[str, str], dict] = {}

    @property
    def spill_path(self) -> Path:
        return self._dir / self.SPILL_NAME

    def __len__(self) -> int:
        return len(self._mem) + len(self._spilled)

    @property
    def in_memory(self) -> int:
        return len(self._mem)

    @property
    def spilled(self) -> int:
        return len(self._spilled)

    def _horizon(self, record: dict) -> int:
        return record["time"] + self._margin(record["side"])

    def add(self, record: dict) -> None:
        key = (record["side"], record["record_id"])
        self._mem[key] = record
        while len(self._mem) > self._cap:
            # 先溢写离释放最远的记录（horizon 最大），它们等待最久。
            victim = max(
                self._mem,
                key=lambda k: (self._horizon(self._mem[k]), self._mem[k]["seq"], k[1]),
            )
            self._spilled[victim] = self._mem.pop(victim)
            self._flush_spill()

    def remove(self, side: str, record_id: str) -> None:
        key = (side, record_id)
        if key in self._mem:
            del self._mem[key]
        elif key in self._spilled:
            del self._spilled[key]
            self._flush_spill()

    def scan(self, side: str, device: str, lo: int, hi: int) -> list[dict]:
        """按 (time, seq, record_id) 确定序返回窗口内的等待记录。"""
        found = [
            record
            for record in list(self._mem.values()) + list(self._spilled.values())
            if record["side"] == side
            and record["device"] == device
            and lo <= record["time"] <= hi
        ]
        found.sort(key=lambda r: (r["time"], r["seq"], r["record_id"]))
        return found

    def records(self, side: str | None = None) -> list[dict]:
        all_records = list(self._mem.values()) + list(self._spilled.values())
        if side is not None:
            all_records = [r for r in all_records if r["side"] == side]
        return sorted(all_records, key=lambda r: (r["seq"], r["record_id"]))

    def snapshot(self) -> list[dict]:
        return self.records()

    def load_snapshot(self, records: list[dict]) -> None:
        self._mem.clear()
        self._spilled.clear()
        self._flush_spill()
        for record in records:
            self.add(dict(record))

    def _flush_spill(self) -> None:
        if self._spilled:
            lines = [
                json.dumps(record, ensure_ascii=False, sort_keys=True)
                for record in sorted(
                    self._spilled.values(), key=lambda r: (r["seq"], r["record_id"])
                )
            ]
            _atomic_write_text(self.spill_path, "\n".join(lines) + "\n")
        else:
            for path in (self.spill_path, self.spill_path.with_name(self.SPILL_NAME + ".tmp")):
                if path.exists():
                    path.unlink()


class EventTimeJoin:
    """双流事件时间联接的核心状态机（不含批提交逻辑）。"""

    def __init__(self, config: JoinConfig, state_dir):
        self._config = config
        self._watermarks: dict[str, int | None] = {LEFT: None, RIGHT: None}
        self._waiting = WaitingStore(config.buffer_max_records, state_dir, config.margin)
        self._registry: dict[str, str] = {}
        self._pair_index: set[tuple[str, str]] = set()
        self._released: set[tuple[str, str]] = set()
        self._released_log: list[dict] = []
        self._seq = 0
        self.duplicates_suppressed = 0

    # ---- 只读观察口 ----

    @property
    def watermarks(self) -> dict:
        return dict(self._watermarks)

    def watermark(self, side: str) -> int | None:
        return self._watermarks[side]

    def waiting_records(self, side: str | None = None) -> list[dict]:
        return self._waiting.records(side)

    @property
    def waiting_in_memory(self) -> int:
        return self._waiting.in_memory

    @property
    def waiting_spilled(self) -> int:
        return self._waiting.spilled

    @property
    def pair_index_size(self) -> int:
        return len(self._pair_index)

    def held_records(self) -> list[dict]:
        """仍在等待的记录及其不能释放的水位依据。"""
        held = []
        for record in self._waiting.records():
            side = record["side"]
            opposite = other_side(side)
            horizon = record["time"] + self._config.margin(side)
            current = self._watermarks[opposite]
            held.append(
                {
                    "side": side,
                    "record_id": record["record_id"],
                    "device": record["device"],
                    "time": record["time"],
                    "horizon": horizon,
                    "opposite_watermark": current,
                    "basis": (
                        f"需 W_{opposite} > {side}.time+{_MARGIN_NAME[side]}={horizon}"
                        f"（严格大于），当前 W_{opposite}={current}"
                    ),
                }
            )
        return held

    # ---- 消息处理 ----

    def process(self, message: dict) -> list[dict]:
        kind = message.get("kind")
        if kind == "record":
            return self._process_record(message)
        if kind == "watermark":
            return self._process_watermark(message)
        raise ValueError(f"未知消息类型: {kind!r}")

    def _process_record(self, message: dict) -> list[dict]:
        side = message["side"]
        record_id = message["record_id"]
        digest = record_digest(message)
        seen = self._registry.get(f"{side}:{record_id}")
        if seen is not None:
            if seen == digest:
                self.duplicates_suppressed += 1
                return []
            return [
                {
                    "type": "reject",
                    "reason": "conflicting_record_id",
                    "side": side,
                    "record_id": record_id,
                    "detail": "record_id 不可变：同 id 不同内容，拒绝",
                }
            ]
        self._registry[f"{side}:{record_id}"] = digest

        current = self._watermarks[side]
        if current is not None and message["time"] < current:
            return [self._late_event(message, current)]

        record = {
            "side": side,
            "record_id": record_id,
            "device": message["device"],
            "time": message["time"],
            "seq": self._seq,
        }
        self._seq += 1
        self._waiting.add(record)

        opposite = other_side(side)
        if side == LEFT:
            lo, hi = record["time"] - self._config.early, record["time"] + self._config.late
        else:
            lo, hi = record["time"] - self._config.late, record["time"] + self._config.early
        events = []
        for candidate in self._waiting.scan(opposite, record["device"], lo, hi):
            left_rec = record if side == LEFT else candidate
            right_rec = candidate if side == LEFT else record
            pair_key = (left_rec["record_id"], right_rec["record_id"])
            if pair_key in self._pair_index:
                continue  # 等待期间不重复输出旧行对
            self._pair_index.add(pair_key)
            events.append(
                {
                    "type": "pair",
                    "left": pair_key[0],
                    "right": pair_key[1],
                    "device": record["device"],
                    "left_time": left_rec["time"],
                    "right_time": right_rec["time"],
                }
            )
        # 即时释放：若当前对侧水位已严格大于本记录 horizon，未来正常记录
        # 不可能再与它匹配，无需占据等待状态。
        opposite_wm = self._watermarks[opposite]
        if opposite_wm is not None and opposite_wm > record["time"] + self._config.margin(side):
            events.append(self._release(side, record, opposite, opposite_wm))
        return events

    def _late_event(self, message: dict, violated: int) -> dict:
        side = message["side"]
        opposite = other_side(side)
        time = message["time"]
        if side == LEFT:
            lo, hi = time - self._config.early, time + self._config.late
        else:
            lo, hi = time - self._config.late, time + self._config.early
        sealed = sorted(
            entry["record_id"]
            for entry in self._released_log
            if entry["side"] == opposite
            and entry["device"] == message["device"]
            and lo <= entry["time"] <= hi
        )
        waiting = [
            record["record_id"]
            for record in self._waiting.scan(opposite, message["device"], lo, hi)
        ]
        return {
            "type": "late",
            "side": side,
            "record_id": message["record_id"],
            "device": message["device"],
            "time": time,
            "violated_watermark": {"side": side, "W": violated},
            "affected_range": {
                "opposite_side": opposite,
                "device": message["device"],
                "min_time": lo,
                "max_time": hi,
            },
            "sealed_opposite_records": sealed,
            "waiting_opposite_records": waiting,
            "opposite_watermark": self._watermarks[opposite],
            "rewrite_sealed_output": False,
        }

    def _process_watermark(self, message: dict) -> list[dict]:
        side = message["side"]
        new_w = message["W"]
        current = self._watermarks[side]
        if current is not None and new_w < current:
            return [
                {
                    "type": "reject",
                    "reason": "watermark_regression",
                    "side": side,
                    "attempted": new_w,
                    "current": current,
                    "detail": "水位每侧单调不减，回退拒绝",
                }
            ]
        self._watermarks[side] = new_w

        victim_side = other_side(side)
        margin = self._config.margin(victim_side)
        releasable = [
            record
            for record in self._waiting.records(victim_side)
            if new_w > record["time"] + margin
        ]
        releasable.sort(key=lambda r: (r["time"] + margin, r["seq"], r["record_id"]))
        return [self._release(victim_side, record, side, new_w) for record in releasable]

    def _release(self, side: str, record: dict, watermark_side: str, watermark: int) -> dict:
        """释放一条等待记录并给出水位依据；调用方需已确认严格大于条件成立。"""
        horizon = record["time"] + self._config.margin(side)
        self._waiting.remove(side, record["record_id"])
        self._released.add((side, record["record_id"]))
        entry = {
            "side": side,
            "record_id": record["record_id"],
            "device": record["device"],
            "time": record["time"],
            "horizon": horizon,
            "watermark_side": watermark_side,
            "watermark": watermark,
        }
        self._released_log.append(entry)
        self._prune_pair_index(side, record["record_id"])
        return {
            "type": "release",
            **entry,
            "rule": "watermark_strictly_greater_than_horizon",
            "basis": f"W_{watermark_side}={watermark} > {side}.time+{_MARGIN_NAME[side]}={horizon}",
        }

    def _prune_pair_index(self, side: str, record_id: str) -> None:
        """两侧成员都已释放的行对不可能再次形成，可从索引清除以保持有界。"""

        def keep(pair: tuple[str, str]) -> bool:
            left_released = (LEFT, pair[0]) in self._released
            right_released = (RIGHT, pair[1]) in self._released
            return not (left_released and right_released)

        self._pair_index = {pair for pair in self._pair_index if keep(pair)}

    # ---- 快照与恢复 ----

    def snapshot(self) -> dict:
        return {
            "watermarks": dict(self._watermarks),
            "waiting": self._waiting.snapshot(),
            "registry": dict(self._registry),
            "pair_index": sorted(self._pair_index),
            "released": sorted(self._released),
            "released_log": list(self._released_log),
            "seq": self._seq,
            "duplicates_suppressed": self.duplicates_suppressed,
        }

    def restore(self, snapshot: dict) -> None:
        self._watermarks = {side: snapshot["watermarks"].get(side) for side in SIDES}
        self._waiting.load_snapshot(snapshot["waiting"])
        self._registry = dict(snapshot["registry"])
        self._pair_index = {tuple(pair) for pair in snapshot["pair_index"]}
        self._released = {tuple(item) for item in snapshot["released"]}
        self._released_log = [dict(entry) for entry in snapshot["released_log"]]
        self._seq = snapshot["seq"]
        self.duplicates_suppressed = snapshot["duplicates_suppressed"]


class CheckpointedRunner:
    """按源批提交输出与检查点，保证输入位置、状态、输出三者一致。

    批提交顺序：先把本批输出追加到 output.jsonl 并 fsync，再原子写检查点
    （记录 last_offset 与 output_committed）。恢复时以最新检查点为准，
    截断 output.jsonl 中未提交的行，从 last_offset 之后继续，不漏不重。
    """

    def __init__(self, config: JoinConfig, workdir):
        self.config = config
        self.workdir = Path(workdir)
        self.checkpoint_dir = self.workdir / "checkpoints"
        self.state_dir = self.workdir / "state"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.output_path = self.workdir / "output.jsonl"
        self.join = EventTimeJoin(config, self.state_dir)
        self._pending: list[dict] = []
        self._committed_outputs = 0
        self._last_offset: int | None = None
        self._dirty = False
        self.counts = {"pair": 0, "late": 0, "release": 0, "reject": 0}

    def process_message(self, message: dict) -> list[dict]:
        events = self.join.process(message)
        offset = message.get("source_offset")
        for event in events:
            event["source_offset"] = offset
            self.counts[event["type"]] = self.counts.get(event["type"], 0) + 1
        self._pending.extend(events)
        if offset is not None:
            self._last_offset = offset
        self._dirty = True
        return events

    def commit(self) -> Path | None:
        if not self._dirty:
            return None
        if self._pending:
            with self.output_path.open("a", encoding="utf-8") as handle:
                for event in self._pending:
                    handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._committed_outputs += len(self._pending)
            self._pending = []
        snapshot = self.join.snapshot()
        snapshot["last_offset"] = self._last_offset
        snapshot["output_committed"] = self._committed_outputs
        seq = len(list(self.checkpoint_dir.glob("checkpoint-*.json")))
        path = self.checkpoint_dir / f"checkpoint-{seq:04d}.json"
        _atomic_write_text(path, json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=1))
        self._dirty = False
        return path

    def run(
        self,
        messages: list[dict],
        checkpoint_offsets=(),
        stop_after_offset: int | None = None,
    ) -> dict:
        marks = set(checkpoint_offsets)
        crashed = False
        for message in messages:
            offset = message.get("source_offset")
            if self._last_offset is not None and offset is not None and offset <= self._last_offset:
                continue  # 恢复后跳过已提交的输入
            if stop_after_offset is not None and offset is not None and offset > stop_after_offset:
                crashed = True
                break  # 模拟故障：当前批不提交，直接停止
            self.process_message(message)
            if offset in marks:
                self.commit()
        if self._dirty and not crashed:
            self.commit()
        return self.summary()

    @classmethod
    def recover(cls, config: JoinConfig, workdir) -> "CheckpointedRunner":
        runner = cls(config, workdir)
        checkpoints = sorted(runner.checkpoint_dir.glob("checkpoint-*.json"))
        committed = 0
        if checkpoints:
            snapshot = json.loads(checkpoints[-1].read_text(encoding="utf-8"))
            runner.join.restore(snapshot)
            runner._last_offset = snapshot["last_offset"]
            committed = snapshot["output_committed"]
        runner._committed_outputs = committed
        runner._truncate_output(committed)
        if runner.output_path.exists():
            for line in runner.output_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    event = json.loads(line)
                    runner.counts[event["type"]] = runner.counts.get(event["type"], 0) + 1
        return runner

    def _truncate_output(self, keep: int) -> None:
        lines = []
        if self.output_path.exists():
            lines = [
                line
                for line in self.output_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        _atomic_write_text(self.output_path, "".join(line + "\n" for line in lines[:keep]))

    def summary(self) -> dict:
        return {
            "counts": dict(self.counts),
            "duplicates_suppressed": self.join.duplicates_suppressed,
            "watermarks": self.join.watermarks,
            "waiting": self.join.held_records(),
            "committed_outputs": self._committed_outputs,
            "last_offset": self._last_offset,
            "output": str(self.output_path),
        }
