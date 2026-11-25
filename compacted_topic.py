"""压缩 topic offset 可见性验证器（所有日志均为模拟数据）。

模拟 Kafka 日志压缩（log compaction）后的 offset 可见状态：
- 重复 key 的旧记录被清理；
- tombstone（value 为 None 的删除标记）有独立的保留窗口，到期后同样被移除；
- 被移除的 offset 形成“空洞”，消费者 offset 可能指向已删除记录；
- 慢消费者的 offset 可能落后于压缩后的 log start offset。

兼容 Python 3.8.2（不使用 3.9+ 语法）。
"""

import json
import sys
from dataclasses import dataclass, field
from enum import Enum


class VisibilityState(Enum):
    """单个 offset 在压缩后的可见状态。"""

    RETAINED = "RETAINED"  # 记录被保留（key 的最新值）
    TOMBSTONE_RETAINED = "TOMBSTONE_RETAINED"  # 墓碑仍在保留窗口内
    COMPACTED_AWAY = "COMPACTED_AWAY"  # 同 key 存在更新记录，旧记录被清理
    TOMBSTONE_EXPIRED = "TOMBSTONE_EXPIRED"  # 墓碑超过保留窗口被移除，形成空洞
    HOLE = "HOLE"  # 输入日志中本就不存在的 offset（段间空洞）


class ConsumerStatus(Enum):
    """消费者（或重试引用）所处位置的健康状态。"""

    OK = "OK"  # 指向一条仍保留的记录
    BEHIND_LOG_START = "BEHIND_LOG_START"  # 慢消费者：落后于 log start offset
    POINTS_AT_REMOVED = "POINTS_AT_REMOVED"  # 指向已被压缩/墓碑到期删除的记录
    POINTS_AT_HOLE = "POINTS_AT_HOLE"  # 指向输入中本不存在的空洞 offset
    BEYOND_LOG_END = "BEYOND_LOG_END"  # 越过日志末端（等待新消息）


class SkipRisk(Enum):
    """重试/跳转消费时可能跳过的内容。"""

    NONE = "NONE"
    SKIPS_SUPERSEDED_VALUE = "SKIPS_SUPERSEDED_VALUE"  # 只跳过被覆盖的旧值，最新值仍在后面
    SKIPS_LATEST_VALUE = "SKIPS_LATEST_VALUE"  # 跳过某 key 真正的最新值
    SKIPS_TOMBSTONE = "SKIPS_TOMBSTONE"  # 跳过已到期墓碑，删除语义丢失，key 残留旧值


@dataclass(frozen=True)
class Record:
    offset: int
    key: str
    value: object  # None 表示 tombstone
    timestamp: int

    @property
    def is_tombstone(self):
        return self.value is None


@dataclass
class ConsumerImpact:
    group: str
    offset: int
    status: ConsumerStatus
    next_readable_offset: object  # int 或 None
    skipped_offsets: list = field(default_factory=list)
    risk: SkipRisk = SkipRisk.NONE
    detail: str = ""

    def to_dict(self):
        return {
            "group": self.group,
            "offset": self.offset,
            "status": self.status.value,
            "next_readable_offset": self.next_readable_offset,
            "skipped_offsets": list(self.skipped_offsets),
            "risk": self.risk.value,
            "detail": self.detail,
        }


class CompactedLog:
    """对一组模拟日志段执行压缩，并回答 offset 可见性查询。"""

    def __init__(self, records, tombstone_retention_ms, now):
        self.tombstone_retention_ms = tombstone_retention_ms
        self.now = now
        self.records = sorted(records, key=lambda r: r.offset)
        offsets = [r.offset for r in self.records]
        if len(set(offsets)) != len(offsets):
            raise ValueError("duplicate offset in input records")
        self._by_offset = dict((r.offset, r) for r in self.records)
        self._retained = {}  # offset -> Record
        self._removed = {}  # offset -> (Record, VisibilityState)
        self._compact()

    def _compact(self):
        latest_by_key = {}
        for rec in self.records:
            latest_by_key[rec.key] = rec.offset
        for rec in self.records:
            if rec.offset != latest_by_key[rec.key]:
                self._removed[rec.offset] = (rec, VisibilityState.COMPACTED_AWAY)
            elif rec.is_tombstone and self.now - rec.timestamp > self.tombstone_retention_ms:
                self._removed[rec.offset] = (rec, VisibilityState.TOMBSTONE_EXPIRED)
            else:
                self._retained[rec.offset] = rec

    # ---- 基础查询 ----

    @property
    def log_start_offset(self):
        return min(self._retained) if self._retained else None

    @property
    def log_end_offset(self):
        """下一条待写入消息的 offset。"""
        return self.records[-1].offset + 1 if self.records else 0

    def state_at(self, offset):
        if offset in self._retained:
            rec = self._retained[offset]
            return (VisibilityState.TOMBSTONE_RETAINED if rec.is_tombstone
                    else VisibilityState.RETAINED)
        if offset in self._removed:
            return self._removed[offset][1]
        return VisibilityState.HOLE

    def visibility_map(self):
        """返回 {offset: 状态名}，覆盖 [最小输入 offset, 最大输入 offset]。"""
        if not self.records:
            return {}
        lo, hi = self.records[0].offset, self.records[-1].offset
        return dict((o, self.state_at(o).value) for o in range(lo, hi + 1))

    # ---- 输出：可跳过范围 / 仍需保留记录 ----

    def skippable_ranges(self):
        """连续不可读 offset 的闭区间列表 [(start, end), ...]。"""
        if not self.records:
            return []
        lo, hi = self.records[0].offset, self.records[-1].offset
        ranges = []
        start = None
        for o in range(lo, hi + 1):
            if o in self._retained:
                if start is not None:
                    ranges.append((start, o - 1))
                    start = None
            elif start is None:
                start = o
        if start is not None:
            ranges.append((start, hi))
        return ranges

    def retained_records(self):
        return [self._retained[o] for o in sorted(self._retained)]

    # ---- 消费者影响分析 ----

    def _next_readable(self, offset):
        for o in sorted(self._retained):
            if o >= offset:
                return o
        return None

    def _latest_retained_offset_for_key(self, key):
        candidates = [o for o, r in self._retained.items() if r.key == key]
        return max(candidates) if candidates else None

    def analyze_consumer(self, group, offset):
        next_readable = self._next_readable(offset)
        if offset in self._retained:
            return ConsumerImpact(group, offset, ConsumerStatus.OK, offset)

        if offset in self._removed:
            rec, state = self._removed[offset]
            skipped = [o for o in range(offset, next_readable)] if next_readable is not None else []
            if state is VisibilityState.TOMBSTONE_EXPIRED:
                risk = SkipRisk.SKIPS_TOMBSTONE
                detail = ("offset %d 的墓碑(key=%s)已过保留窗口被移除；重试将跳到 %s，"
                          "删除语义丢失，下游可能残留该 key 的旧值"
                          % (offset, rec.key, next_readable))
            else:
                latest_visible = self._latest_retained_offset_for_key(rec.key)
                if latest_visible is not None:
                    risk = SkipRisk.SKIPS_SUPERSEDED_VALUE
                    detail = ("offset %d 的值(key=%s)已被更新值覆盖；最新值仍在 offset %d，"
                              "跳过不影响最终状态" % (offset, rec.key, latest_visible))
                else:
                    risk = SkipRisk.SKIPS_LATEST_VALUE
                    detail = ("offset %d 之后 key=%s 已无任何保留记录；重试跳到 %s 将跳过真正的最新值"
                              % (offset, rec.key, next_readable))
            return ConsumerImpact(group, offset, ConsumerStatus.POINTS_AT_REMOVED,
                                  next_readable, skipped, risk, detail)

        if offset not in self._by_offset:
            if self.records and offset < self.records[0].offset:
                skipped = [o for o in sorted(self._removed)
                           if next_readable is None or o < next_readable]
                if any(self._removed[o][1] is VisibilityState.TOMBSTONE_EXPIRED for o in skipped):
                    risk = SkipRisk.SKIPS_TOMBSTONE
                elif skipped:
                    risk = SkipRisk.SKIPS_SUPERSEDED_VALUE
                else:
                    risk = SkipRisk.NONE
                return ConsumerImpact(
                    group, offset, ConsumerStatus.BEHIND_LOG_START, next_readable, skipped,
                    risk,
                    detail="慢消费者：offset %d 落后于 log start offset %s"
                           % (offset, self.log_start_offset))
            if self.records and offset > self.records[-1].offset:
                return ConsumerImpact(group, offset, ConsumerStatus.BEYOND_LOG_END, None,
                                      detail="offset 越过日志末端，等待新消息")
            skipped = [o for o in range(offset, next_readable)] if next_readable is not None else []
            return ConsumerImpact(group, offset, ConsumerStatus.POINTS_AT_HOLE,
                                  next_readable, skipped, SkipRisk.NONE,
                                  "offset %d 位于输入日志的空洞中" % offset)

        # offset 存在但既未保留也未移除（理论不可达）
        return ConsumerImpact(group, offset, ConsumerStatus.OK, offset)

    def affected_consumers(self, consumers):
        """consumers: [{"group":..., "offset":..., "retry_offset":...可选}]"""
        impacts = []
        for c in consumers:
            impacts.append(self.analyze_consumer(c["group"], c["offset"]))
            retry = c.get("retry_offset")
            if retry is not None:
                impact = self.analyze_consumer(c["group"], retry)
                impact.detail = "[retry_offset] " + impact.detail
                impacts.append(impact)
        return [i for i in impacts if i.status is not ConsumerStatus.OK]


# ---- 输入/输出 ----

def load_scenario(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    records = []
    for seg in data.get("segments", []):
        for r in seg.get("records", []):
            records.append(Record(offset=r["offset"], key=r["key"],
                                  value=r.get("value"), timestamp=r["timestamp"]))
    log = CompactedLog(records,
                       tombstone_retention_ms=data["tombstone_retention_ms"],
                       now=data["now"])
    return log, data.get("consumers", [])


def build_report(log, consumers):
    return {
        "log_start_offset": log.log_start_offset,
        "log_end_offset": log.log_end_offset,
        "visibility": log.visibility_map(),
        "skippable_ranges": [list(r) for r in log.skippable_ranges()],
        "retained_records": [
            {"offset": r.offset, "key": r.key, "value": r.value, "timestamp": r.timestamp}
            for r in log.retained_records()
        ],
        "affected_consumers": [i.to_dict() for i in log.affected_consumers(consumers)],
    }


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 1:
        sys.stderr.write("usage: python compacted_topic.py <scenario.json>\n")
        return 2
    log, consumers = load_scenario(argv[0])
    json.dump(build_report(log, consumers), sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
