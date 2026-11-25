"""压缩 topic offset 可见性验证器的单元测试（全部使用模拟日志数据）。"""

import unittest

from compacted_topic import (
    CompactedLog,
    ConsumerStatus,
    Record,
    SkipRisk,
    VisibilityState,
)


def rec(offset, key, value, ts=0):
    return Record(offset=offset, key=key, value=value, timestamp=ts)


class DuplicateKeyCompactionTest(unittest.TestCase):
    """重复 key：旧值被清理，最新值保留。"""

    def setUp(self):
        self.log = CompactedLog(
            records=[
                rec(0, "a", "a1"),
                rec(1, "b", "b1"),
                rec(2, "a", "a2"),
                rec(3, "a", "a3"),
            ],
            tombstone_retention_ms=1000,
            now=10000,
        )

    def test_old_values_compacted_away(self):
        self.assertEqual(self.log.state_at(0), VisibilityState.COMPACTED_AWAY)
        self.assertEqual(self.log.state_at(2), VisibilityState.COMPACTED_AWAY)

    def test_latest_values_retained(self):
        self.assertEqual(self.log.state_at(1), VisibilityState.RETAINED)
        self.assertEqual(self.log.state_at(3), VisibilityState.RETAINED)
        self.assertEqual([r.offset for r in self.log.retained_records()], [1, 3])

    def test_skippable_ranges(self):
        self.assertEqual(self.log.skippable_ranges(), [(0, 0), (2, 2)])

    def test_consumer_on_compacted_value_skips_only_superseded(self):
        impact = self.log.analyze_consumer("g1", 2)
        self.assertEqual(impact.status, ConsumerStatus.POINTS_AT_REMOVED)
        self.assertEqual(impact.risk, SkipRisk.SKIPS_SUPERSEDED_VALUE)
        self.assertEqual(impact.next_readable_offset, 3)
        self.assertEqual(impact.skipped_offsets, [2])

    def test_consumer_on_retained_value_ok(self):
        impact = self.log.analyze_consumer("g1", 3)
        self.assertEqual(impact.status, ConsumerStatus.OK)
        self.assertEqual(impact.next_readable_offset, 3)


class TombstoneRetentionTest(unittest.TestCase):
    """墓碑清理：窗口内保留，到期后移除并形成空洞。"""

    def build(self, now):
        return CompactedLog(
            records=[
                rec(0, "a", "a1", ts=0),
                rec(1, "a", None, ts=500),
                rec(2, "b", "b1", ts=0),
                rec(3, "c", None, ts=9500),
            ],
            tombstone_retention_ms=1000,
            now=now,
        )

    def test_tombstone_within_window_retained(self):
        log = self.build(now=1200)
        self.assertEqual(log.state_at(1), VisibilityState.TOMBSTONE_RETAINED)
        self.assertEqual(log.state_at(0), VisibilityState.COMPACTED_AWAY)

    def test_tombstone_expired_becomes_hole(self):
        log = self.build(now=2000)
        self.assertEqual(log.state_at(1), VisibilityState.TOMBSTONE_EXPIRED)
        self.assertEqual(log.state_at(3), VisibilityState.TOMBSTONE_RETAINED)
        self.assertEqual(log.skippable_ranges(), [(0, 1)])
        self.assertEqual([r.offset for r in log.retained_records()], [2, 3])

    def test_retry_past_expired_tombstone_loses_delete(self):
        log = self.build(now=2000)
        impact = log.analyze_consumer("g-retry", 1)
        self.assertEqual(impact.status, ConsumerStatus.POINTS_AT_REMOVED)
        self.assertEqual(impact.risk, SkipRisk.SKIPS_TOMBSTONE)
        self.assertEqual(impact.next_readable_offset, 2)

    def test_retry_offset_reported_separately(self):
        log = self.build(now=2000)
        consumers = [{"group": "g1", "offset": 2, "retry_offset": 1}]
        impacts = log.affected_consumers(consumers)
        self.assertEqual(len(impacts), 1)
        self.assertEqual(impacts[0].offset, 1)
        self.assertTrue(impacts[0].detail.startswith("[retry_offset]"))


class OffsetHoleTest(unittest.TestCase):
    """offset 空洞：输入段之间本就不存在的 offset。"""

    def setUp(self):
        self.log = CompactedLog(
            records=[
                rec(0, "a", "a1"),
                rec(1, "b", "b1"),
                rec(2, "c", "c1"),
                rec(5, "d", "d1"),
                rec(6, "e", "e1"),
            ],
            tombstone_retention_ms=1000,
            now=0,
        )

    def test_gap_offsets_are_holes(self):
        self.assertEqual(self.log.state_at(3), VisibilityState.HOLE)
        self.assertEqual(self.log.state_at(4), VisibilityState.HOLE)

    def test_visibility_map_covers_holes(self):
        vis = self.log.visibility_map()
        self.assertEqual(vis[3], "HOLE")
        self.assertEqual(vis[4], "HOLE")
        self.assertEqual(vis[5], "RETAINED")

    def test_consumer_in_hole_skips_to_next_readable(self):
        impact = self.log.analyze_consumer("g1", 3)
        self.assertEqual(impact.status, ConsumerStatus.POINTS_AT_HOLE)
        self.assertEqual(impact.next_readable_offset, 5)
        self.assertEqual(impact.skipped_offsets, [3, 4])
        self.assertEqual(impact.risk, SkipRisk.NONE)

    def test_beyond_log_end(self):
        impact = self.log.analyze_consumer("g1", 7)
        self.assertEqual(impact.status, ConsumerStatus.BEYOND_LOG_END)
        self.assertIsNone(impact.next_readable_offset)


class SlowConsumerTest(unittest.TestCase):
    """慢消费者：offset 落后于压缩后的 log start offset。"""

    def setUp(self):
        self.log = CompactedLog(
            records=[
                rec(2, "a", "a1", ts=0),
                rec(3, "a", "a2", ts=0),
                rec(4, "b", None, ts=0),
                rec(5, "c", "c1", ts=0),
            ],
            tombstone_retention_ms=100,
            now=10000,
        )

    def test_log_start_offset(self):
        self.assertEqual(self.log.log_start_offset, 3)
        self.assertEqual(self.log.log_end_offset, 6)

    def test_slow_consumer_behind_log_start(self):
        impact = self.log.analyze_consumer("slow-g", 0)
        self.assertEqual(impact.status, ConsumerStatus.BEHIND_LOG_START)
        self.assertEqual(impact.next_readable_offset, 3)
        self.assertEqual(impact.skipped_offsets, [2])
        self.assertEqual(impact.risk, SkipRisk.SKIPS_SUPERSEDED_VALUE)

    def test_slow_consumer_pointing_at_expired_tombstone(self):
        impact = self.log.analyze_consumer("slow-g", 4)
        self.assertEqual(impact.status, ConsumerStatus.POINTS_AT_REMOVED)
        self.assertEqual(impact.risk, SkipRisk.SKIPS_TOMBSTONE)

    def test_affected_consumers_filters_ok(self):
        consumers = [
            {"group": "ok-g", "offset": 5},
            {"group": "slow-g", "offset": 0},
        ]
        impacts = self.log.affected_consumers(consumers)
        self.assertEqual([i.group for i in impacts], ["slow-g"])


class EdgeCaseTest(unittest.TestCase):
    def test_duplicate_offset_rejected(self):
        with self.assertRaises(ValueError):
            CompactedLog([rec(0, "a", "x"), rec(0, "a", "y")], 1000, 0)

    def test_empty_log(self):
        log = CompactedLog([], tombstone_retention_ms=1000, now=0)
        self.assertIsNone(log.log_start_offset)
        self.assertEqual(log.log_end_offset, 0)
        self.assertEqual(log.visibility_map(), {})
        self.assertEqual(log.skippable_ranges(), [])


if __name__ == "__main__":
    unittest.main()
