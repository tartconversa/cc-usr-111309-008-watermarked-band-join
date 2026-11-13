"""有界事件时间联接组件的验证测试。

覆盖：同刻、空流、水位回退拒绝、不同设备、重送（幂等与冲突）、迟到旁路不改写
封存输出、释放严格大于、不臆造水位、内存上限与磁盘溢写、故障恢复不漏不重，
并用有限完整输入与独立全量参考核对正常域全部行对。
"""
import json
import random
import tempfile
import unittest
from pathlib import Path

from src.band_reader import load
from src.event_time_join import (
    LEFT,
    RIGHT,
    SIDES,
    CheckpointedRunner,
    EventTimeJoin,
    JoinConfig,
)

ROOT = Path(__file__).resolve().parents[1]
EARLY, LATE = 2, 3


def record(offset, side, rid, device, time):
    return {
        "source_offset": offset,
        "kind": "record",
        "side": side,
        "record_id": rid,
        "device": device,
        "time": time,
    }


def watermark(offset, side, w):
    return {"source_offset": offset, "kind": "watermark", "side": side, "W": w}


def reference_model(messages, early=EARLY, late=LATE):
    """独立重放语义契约，返回 (正常域行对集合, 迟到id列表, 拒绝列表)。"""
    watermarks = {LEFT: None, RIGHT: None}
    registry = {}
    normal = {LEFT: [], RIGHT: []}
    late_ids, rejects = [], []
    for message in messages:
        if message["kind"] == "watermark":
            current = watermarks[message["side"]]
            if current is not None and message["W"] < current:
                rejects.append(("watermark_regression", message["side"], message["W"]))
                continue
            watermarks[message["side"]] = message["W"]
            continue
        key = (message["side"], message["record_id"])
        content = (message["device"], message["time"])
        if key in registry:
            if registry[key] != content:
                rejects.append(("conflicting_record_id", message["side"], message["record_id"]))
            continue
        registry[key] = content
        current = watermarks[message["side"]]
        if current is not None and message["time"] < current:
            late_ids.append(message["record_id"])
            continue
        normal[message["side"]].append(message)
    pairs = set()
    for left in normal[LEFT]:
        for right in normal[RIGHT]:
            if left["device"] == right["device"] and -early <= right["time"] - left["time"] <= late:
                pairs.add((left["record_id"], right["record_id"]))
    return pairs, late_ids, rejects


def committed_events(runner):
    if not runner.output_path.exists():
        return []
    return [
        json.loads(line)
        for line in runner.output_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def pairs_of(events):
    return {(event["left"], event["right"]) for event in events if event["type"] == "pair"}


def run_to_end(messages, config=None, checkpoint_offsets=()):
    config = config or JoinConfig(early=EARLY, late=LATE, buffer_max_records=64)
    directory = tempfile.TemporaryDirectory()
    runner = CheckpointedRunner(config, directory.name)
    runner.run(messages, checkpoint_offsets)
    return runner, directory


def generated_stream(seed=20261113):
    """确定性生成有限完整输入：多设备、同刻、迟到、重送、冲突、水位回退。"""
    rng = random.Random(seed)
    messages, sent = [], []
    watermarks = {LEFT: 0, RIGHT: 0}
    counters = {LEFT: 0, RIGHT: 0}
    offset = 0
    for _ in range(160):
        roll = rng.random()
        if roll < 0.60 or not sent:
            side = rng.choice(SIDES)
            counters[side] += 1
            time = watermarks[side] + rng.randint(0, 5)
            if rng.random() < 0.15:
                time = max(0, watermarks[side] - rng.randint(1, 4))  # 迟到
            message = record(
                offset, side, f"{side}{counters[side]}", rng.choice(["a", "b", "c"]), time
            )
            messages.append(message)
            sent.append(message)
        elif roll < 0.75:
            original = rng.choice(sent)
            message = dict(original)
            message["source_offset"] = offset
            if rng.random() < 0.4:
                message["time"] = original["time"] + rng.randint(1, 9)  # 冲突重送
            messages.append(message)
        else:
            side = rng.choice(SIDES)
            new_w = watermarks[side] + rng.randint(0, 3)
            if rng.random() < 0.12:
                new_w = max(0, watermarks[side] - rng.randint(1, 2))  # 回退
            messages.append(watermark(offset, side, new_w))
            if new_w >= watermarks[side]:
                watermarks[side] = new_w
        offset += 1
    for side in SIDES:  # 末尾大水位使全部等待状态可释放
        messages.append(watermark(offset, side, 10**6))
        offset += 1
    return messages


class FixtureEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.band = load(ROOT / "fixtures/band.json")
        self.messages = load(ROOT / "fixtures/stream.jsonl")
        self.config = JoinConfig.from_band(self.band)

    def test_fixture_pairs_late_releases(self):
        runner, directory = run_to_end(
            self.messages, self.config, self.band["checkpoint_offsets"]
        )
        self.addCleanup(directory.cleanup)
        events = committed_events(runner)

        # 左 t=10 与右 t=8、13 均匹配（边界包含），输出全部行对
        self.assertEqual(pairs_of(events), {("l1", "r1"), ("l1", "r2")})

        # 右 t=13 在 W_R=14 之后到达：13 < 14 进 late，记录被违反水位与影响范围
        late = [event for event in events if event["type"] == "late"]
        self.assertEqual(len(late), 1)
        self.assertEqual(late[0]["record_id"], "late13")
        self.assertEqual(late[0]["violated_watermark"], {"side": "R", "W": 14})
        self.assertEqual(late[0]["affected_range"]["min_time"], 10)
        self.assertEqual(late[0]["affected_range"]["max_time"], 15)
        self.assertEqual(late[0]["sealed_opposite_records"], ["l1"])
        self.assertFalse(late[0]["rewrite_sealed_output"])

        # 释放依据：l1 由 W_R=14 > 10+late=13 释放；r1、r2 由 W_L=100 释放
        releases = {event["record_id"]: event for event in events if event["type"] == "release"}
        self.assertEqual(set(releases), {"l1", "r1", "r2"})
        self.assertEqual(releases["l1"]["watermark_side"], "R")
        self.assertEqual(releases["l1"]["watermark"], 14)
        self.assertEqual(releases["l1"]["horizon"], 13)
        self.assertEqual(releases["l1"]["rule"], "watermark_strictly_greater_than_horizon")
        self.assertEqual(releases["r1"]["watermark"], 100)
        self.assertEqual(releases["r2"]["watermark"], 100)

        # 末尾水位 100/100 后等待状态清空，行对索引已剪枝
        self.assertEqual(runner.join.held_records(), [])
        self.assertEqual(runner.join.pair_index_size, 0)

        # 与独立全量参考核对正常域全部行对
        ref_pairs, ref_late, ref_rejects = reference_model(self.messages)
        self.assertEqual(pairs_of(events), ref_pairs)
        self.assertEqual([event["record_id"] for event in late], ref_late)
        self.assertEqual(ref_rejects, [])

    def test_fixture_checkpoints_consistent(self):
        runner, directory = run_to_end(
            self.messages, self.config, self.band["checkpoint_offsets"]
        )
        self.addCleanup(directory.cleanup)
        checkpoints = sorted((Path(directory.name) / "checkpoints").glob("checkpoint-*.json"))
        self.assertEqual(len(checkpoints), 3)
        first = json.loads(checkpoints[0].read_text(encoding="utf-8"))
        self.assertEqual(first["last_offset"], 2)
        self.assertEqual(first["watermarks"], {"L": None, "R": 13})
        self.assertEqual(first["output_committed"], 1)  # 仅行对 (l1, r1)
        self.assertEqual(
            {(r["side"], r["record_id"]) for r in first["waiting"]},
            {("L", "l1"), ("R", "r1")},
        )
        last = json.loads(checkpoints[-1].read_text(encoding="utf-8"))
        self.assertEqual(last["last_offset"], 7)
        self.assertEqual(last["waiting"], [])


class JoinSemanticsTests(unittest.TestCase):
    def test_same_timestamp_and_boundaries(self):
        messages = [
            record(0, "L", "l1", "d", 10),
            record(1, "R", "r_same", "d", 10),  # 同刻，差 0
            record(2, "R", "r_lo", "d", 8),  # 差 -2 = -early，边界包含
            record(3, "R", "r_hi", "d", 13),  # 差 +3 = +late，边界包含
            record(4, "R", "r_out_lo", "d", 7),  # 差 -3，出带
            record(5, "R", "r_out_hi", "d", 14),  # 差 +4，出带（题设不匹配用例）
        ]
        runner, directory = run_to_end(messages)
        self.addCleanup(directory.cleanup)
        self.assertEqual(
            pairs_of(committed_events(runner)),
            {("l1", "r_same"), ("l1", "r_lo"), ("l1", "r_hi")},
        )

    def test_same_timestamp_right_arrives_first(self):
        messages = [record(0, "R", "r1", "d", 10), record(1, "L", "l1", "d", 10)]
        runner, directory = run_to_end(messages)
        self.addCleanup(directory.cleanup)
        self.assertEqual(pairs_of(committed_events(runner)), {("l1", "r1")})

    def test_one_record_matches_all_candidates_not_nearest(self):
        messages = [record(0, "L", "l1", "d", 10)]
        messages += [record(i + 1, "R", f"r{t}", "d", t) for i, t in enumerate([8, 9, 10, 11, 12, 13])]
        runner, directory = run_to_end(messages)
        self.addCleanup(directory.cleanup)
        self.assertEqual(
            pairs_of(committed_events(runner)),
            {("l1", f"r{t}") for t in [8, 9, 10, 11, 12, 13]},
        )

    def test_different_devices_never_match(self):
        messages = [
            record(0, "L", "l1", "a", 10),
            record(1, "R", "r1", "b", 10),  # 同刻不同设备
            record(2, "R", "r2", "a", 10),
        ]
        runner, directory = run_to_end(messages)
        self.addCleanup(directory.cleanup)
        self.assertEqual(pairs_of(committed_events(runner)), {("l1", "r2")})

    def test_empty_stream_and_watermark_only(self):
        runner, directory = run_to_end([])
        self.addCleanup(directory.cleanup)
        self.assertEqual(committed_events(runner), [])

        runner2, directory2 = run_to_end([watermark(0, "L", 5), watermark(1, "R", 7)])
        self.addCleanup(directory2.cleanup)
        self.assertEqual(committed_events(runner2), [])
        self.assertEqual(runner2.join.watermarks, {"L": 5, "R": 7})

    def test_watermark_regression_rejected_equal_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(JoinConfig(early=EARLY, late=LATE), directory)
            self.assertEqual(join.process(watermark(0, "L", 10)), [])
            self.assertEqual(join.process(record(1, "L", "l_at_w", "d", 10)), [])  # 恰好 W 仍正常
            rejected = join.process(watermark(2, "L", 9))
            self.assertEqual(rejected[0]["type"], "reject")
            self.assertEqual(rejected[0]["reason"], "watermark_regression")
            self.assertEqual(join.watermark("L"), 10)  # 水位未被回退
            late = join.process(record(3, "L", "l_late", "d", 9))
            self.assertEqual(late[0]["type"], "late")  # 9 < W_L=10
            self.assertEqual(join.process(watermark(4, "L", 10)), [])  # 相等允许
            self.assertEqual(join.process(watermark(5, "L", 11)), [])
            self.assertEqual(join.watermark("L"), 11)

    def test_no_fabricated_watermark_from_max_seen_time(self):
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(JoinConfig(early=EARLY, late=LATE), directory)
            join.process(record(0, "L", "l1", "d", 100))
            events = join.process(record(1, "L", "l2", "d", 50))
            # 没有显式水位，l2 时间倒退也仍是正常记录，不得臆造水位判迟到
            self.assertEqual(events, [])
            self.assertIsNone(join.watermark("L"))
            join.process(watermark(2, "L", 60))
            late = join.process(record(3, "L", "l3", "d", 55))
            self.assertEqual(late[0]["type"], "late")

    def test_release_requires_strictly_greater_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(JoinConfig(early=EARLY, late=LATE), directory)
            join.process(record(0, "L", "l1", "d", 10))
            # W_R=13 等于 horizon=10+late，不释放；边界匹配仍可能到达
            self.assertEqual(join.process(watermark(1, "R", 13)), [])
            self.assertEqual([r["record_id"] for r in join.waiting_records("L")], ["l1"])
            held = join.held_records()[0]
            self.assertEqual(held["horizon"], 13)
            self.assertEqual(held["opposite_watermark"], 13)
            # 等于水位时仍正常接受，且边界匹配成立
            pairs = join.process(record(2, "R", "r_boundary", "d", 13))
            self.assertEqual([(e["left"], e["right"]) for e in pairs], [("l1", "r_boundary")])
            # W_R=14 严格大于 13，释放并给出依据
            releases = join.process(watermark(3, "R", 14))
            self.assertEqual(releases[0]["type"], "release")
            self.assertEqual(releases[0]["record_id"], "l1")
            self.assertEqual(join.waiting_records("L"), [])

            join.process(record(4, "R", "r1", "d", 20))
            # W_L=22 释放 r_boundary（13+early=15 < 22），但 22 == 20+early，r1 不释放
            releases = join.process(watermark(5, "L", 22))
            self.assertEqual([e["record_id"] for e in releases], ["r_boundary"])
            self.assertEqual([r["record_id"] for r in join.waiting_records("R")], ["r1"])
            releases = join.process(watermark(6, "L", 23))  # 23 > 22，释放
            self.assertEqual([e["record_id"] for e in releases], ["r1"])

    def test_resend_idempotent_and_conflict_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(JoinConfig(early=EARLY, late=LATE), directory)
            join.process(record(0, "L", "l1", "d", 10))
            pairs = join.process(record(1, "R", "r1", "d", 10))
            self.assertEqual(len(pairs), 1)
            # 相同内容重送：幂等，无新输出
            self.assertEqual(join.process(record(2, "L", "l1", "d", 10)), [])
            self.assertEqual(join.process(record(3, "R", "r1", "d", 10)), [])
            self.assertEqual(join.duplicates_suppressed, 2)
            # 冲突内容重送：拒绝
            rejected = join.process(record(4, "L", "l1", "d", 99))
            self.assertEqual(rejected[0]["type"], "reject")
            self.assertEqual(rejected[0]["reason"], "conflicting_record_id")
            # 释放后重送仍幂等，不因时间低于水位被误判迟到
            join.process(watermark(5, "R", 100))
            join.process(watermark(6, "L", 100))
            self.assertEqual(join.process(record(7, "L", "l1", "d", 10)), [])
            self.assertEqual(join.duplicates_suppressed, 3)
            self.assertEqual(join.waiting_records(), [])

    def test_late_record_does_not_rewrite_sealed_output(self):
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(JoinConfig(early=EARLY, late=LATE), directory)
            join.process(record(0, "L", "l1", "d", 10))
            join.process(record(1, "R", "r1", "d", 10))
            join.process(watermark(2, "R", 14))  # l1 释放，行对 (l1, r1) 已封存
            late = join.process(record(3, "R", "r_late", "d", 13))  # 13 < 14
            self.assertEqual(late[0]["type"], "late")
            self.assertEqual(late[0]["sealed_opposite_records"], ["l1"])
            # 迟到记录不进入等待状态，未来记录也匹配不到它
            self.assertEqual([r["record_id"] for r in join.waiting_records("R")], ["r1"])
            future = join.process(record(4, "L", "l2", "d", 13))
            self.assertEqual(future, [])  # 不会与 r_late 形成行对


class RecoveryTests(unittest.TestCase):
    def _run_baseline(self, messages, config, checkpoints):
        directory = tempfile.TemporaryDirectory()
        runner = CheckpointedRunner(config, directory.name)
        runner.run(messages, checkpoints)
        lines = runner.output_path.read_text(encoding="utf-8").splitlines()
        return lines, directory

    def _run_with_crash(self, messages, config, checkpoints, stop_after, inject_junk=True):
        directory = tempfile.TemporaryDirectory()
        crashed = CheckpointedRunner(config, directory.name)
        crashed.run(messages, checkpoints, stop_after_offset=stop_after)
        if inject_junk:
            # 模拟崩溃残留的未提交输出（部分行）
            with open(Path(directory.name) / "output.jsonl", "a", encoding="utf-8") as handle:
                handle.write('{"type":"pair","left":"ghost"')
                handle.write("\n")
        recovered = CheckpointedRunner.recover(config, directory.name)
        recovered.run(messages, checkpoints)
        lines = recovered.output_path.read_text(encoding="utf-8").splitlines()
        return lines, recovered, directory

    def test_fixture_crash_recovery_no_loss_no_dup(self):
        band = load(ROOT / "fixtures/band.json")
        config = JoinConfig.from_band(band)
        messages = load(ROOT / "fixtures/stream.jsonl")
        checkpoints = band["checkpoint_offsets"]
        baseline, base_dir = self._run_baseline(messages, config, checkpoints)
        self.addCleanup(base_dir.cleanup)
        # 在 offset 5 后崩溃：5 属于未提交批（检查点在 2/4/7），恢复后重放
        recovered_lines, recovered, rec_dir = self._run_with_crash(
            messages, config, checkpoints, stop_after=5
        )
        self.addCleanup(rec_dir.cleanup)
        self.assertEqual(recovered_lines, baseline)
        self.assertEqual(pairs_of(committed_events(recovered)), {("l1", "r1"), ("l1", "r2")})

    def test_generated_stream_crash_recovery_with_spill(self):
        config = JoinConfig(early=EARLY, late=LATE, buffer_max_records=7)
        messages = generated_stream()
        checkpoints = [m["source_offset"] for m in messages if m["source_offset"] % 13 == 0]
        baseline, base_dir = self._run_baseline(messages, config, checkpoints)
        self.addCleanup(base_dir.cleanup)
        recovered_lines, recovered, rec_dir = self._run_with_crash(
            messages, config, checkpoints, stop_after=30
        )
        self.addCleanup(rec_dir.cleanup)
        self.assertEqual(recovered_lines, baseline)  # 不漏不重，逐行一致
        self.assertEqual(recovered.join.held_records(), [])

    def test_recovery_without_any_checkpoint_starts_clean(self):
        config = JoinConfig(early=EARLY, late=LATE)
        messages = [record(0, "L", "l1", "d", 10), record(1, "R", "r1", "d", 10)]
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        crashed = CheckpointedRunner(config, directory.name)
        crashed.run(messages, stop_after_offset=0)  # 未提交任何检查点
        recovered = CheckpointedRunner.recover(config, directory.name)
        recovered.run(messages)
        self.assertEqual(pairs_of(committed_events(recovered)), {("l1", "r1")})


class FullReferenceTests(unittest.TestCase):
    def test_generated_stream_matches_reference(self):
        config = JoinConfig(early=EARLY, late=LATE, buffer_max_records=64)
        messages = generated_stream()
        runner, directory = run_to_end(
            messages, config, [m["source_offset"] for m in messages if m["source_offset"] % 13 == 0]
        )
        self.addCleanup(directory.cleanup)
        events = committed_events(runner)

        ref_pairs, ref_late, ref_rejects = reference_model(messages)
        pair_events = [e for e in events if e["type"] == "pair"]
        self.assertEqual(pairs_of(events), ref_pairs)  # 正常域全部行对一致
        self.assertEqual(len(pair_events), len(pairs_of(events)))  # 旧行对不重复输出
        self.assertEqual(
            [e["record_id"] for e in events if e["type"] == "late"], ref_late
        )
        got_rejects = [
            (e["reason"], e["side"], e.get("attempted", e.get("record_id")))
            for e in events
            if e["type"] == "reject"
        ]
        self.assertEqual(got_rejects, ref_rejects)
        self.assertEqual(runner.join.held_records(), [])  # 末尾大水位后全部释放

    def test_release_invariant_holds_after_every_message(self):
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(JoinConfig(early=EARLY, late=LATE), directory)
            for message in generated_stream(seed=7):
                join.process(message)
                for record_ in join.waiting_records(LEFT):
                    w = join.watermark(RIGHT)
                    self.assertTrue(w is None or w <= record_["time"] + LATE)
                for record_ in join.waiting_records(RIGHT):
                    w = join.watermark(LEFT)
                    self.assertTrue(w is None or w <= record_["time"] + EARLY)


class BufferSpillTests(unittest.TestCase):
    def test_memory_bounded_and_spill_preserves_pairs(self):
        config = JoinConfig(early=EARLY, late=LATE, buffer_max_records=2)
        messages = [record(i, "L", f"l{i}", "a", 10 + i) for i in range(5)]
        messages += [record(5 + i, "R", f"r{i}", "a", 10 + i) for i in range(5)]
        messages += [watermark(10, "L", 100), watermark(11, "R", 100)]
        with tempfile.TemporaryDirectory() as directory:
            join = EventTimeJoin(config, directory)
            max_mem, spilled_ever = 0, False
            all_events = []
            for message in messages:
                all_events.extend(join.process(message))
                max_mem = max(max_mem, join.waiting_in_memory)
                spilled_ever = spilled_ever or join.waiting_spilled > 0
            self.assertLessEqual(max_mem, 2)  # 内存等待状态有界
            self.assertTrue(spilled_ever)  # 溢写确实发生
            got = {(e["left"], e["right"]) for e in all_events if e["type"] == "pair"}
            ref_pairs, _, _ = reference_model(messages)
            self.assertEqual(got, ref_pairs)  # 溢写不丢行对
            self.assertEqual(join.waiting_records(), [])

    def test_snapshot_restore_with_spilled_state(self):
        config = JoinConfig(early=EARLY, late=LATE, buffer_max_records=2)
        messages = [record(i, "L", f"l{i}", "a", 10 + i) for i in range(5)]
        messages += [record(5 + i, "R", f"r{i}", "a", 10 + i) for i in range(5)]
        with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
            join_a = EventTimeJoin(config, dir_a)
            for message in messages[:3]:
                join_a.process(message)
            snapshot = join_a.snapshot()
            join_b = EventTimeJoin(config, dir_b)
            join_b.restore(snapshot)
            events_a, events_b = [], []
            for message in messages[3:]:
                events_a.extend(join_a.process(message))
                events_b.extend(join_b.process(message))
            self.assertEqual(events_a, events_b)
            self.assertGreaterEqual(join_b.waiting_spilled, 0)


if __name__ == "__main__":
    unittest.main()
