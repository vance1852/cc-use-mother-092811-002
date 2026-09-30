"""统一时间线上的确定性投影与换装排程引擎。

投影只依赖注册表与已入库事件：事件按 (occurred_at, sequence, event_id)
排序后折叠，因此消息乱序入库不会改变结果；相同事件集合重放得到相同
方案。设备故障/恢复在折叠时配对成封闭区间，封锁窗口来自注册表。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .errors import ValidationError
from .hub_events import ARRIVAL_TYPES, format_ts

INF = datetime(9999, 12, 31, tzinfo=None)
SIXTY_MINUTES = timedelta(minutes=60)
MAX_SCHEDULE_STEPS = 1000
LARGE_RANK = 10**9


@dataclass(frozen=True)
class Interval:
    """资源不可用或被占用的半开区间 [start, end)，end 为 None 表示永久。"""

    start: datetime
    end: datetime | None
    source: str
    ref: str


@dataclass
class ContainerState:
    container_id: str
    shipment_id: str
    group_id: str
    position: int
    arrival_at: datetime | None = None
    skipped: bool = False
    transferred_at: datetime | None = None


@dataclass
class ShipmentState:
    shipment_id: str
    batch_id: str
    contract_id: str | None
    priority_rank: int | None
    inbound_party: str
    outbound_party: str
    duration_minutes: int
    containers: dict[str, ContainerState] = field(default_factory=dict)

    @property
    def group_ids(self) -> list[str]:
        return sorted({c.group_id for c in self.containers.values()})


@dataclass(frozen=True)
class RawEvent:
    event_type: str
    occurred_at: datetime
    event_id: str
    sequence: int
    payload: dict[str, Any]


@dataclass
class Projection:
    """某次折叠后的完整只读状态。"""

    site_id: str
    cranes: list[str]
    slots: list[str]
    shipments: dict[str, ShipmentState]
    batch_planned: dict[str, datetime]
    batch_arrival: dict[str, datetime]
    unavailable: dict[str, list[Interval]]
    holds: dict[str, list[Interval]]
    event_count: int

    def conservation_report(self) -> dict[str, Any]:
        """核对每票货物的数量守恒：登记=在途+到达待转+已交接+甩箱。"""

        report = {}
        for shipment in self.shipments.values():
            in_transit = arrived_pending = transferred = skipped = 0
            for box in shipment.containers.values():
                if box.skipped:
                    skipped += 1
                elif box.transferred_at is not None:
                    transferred += 1
                elif box.arrival_at is not None:
                    arrived_pending += 1
                else:
                    in_transit += 1
            total = len(shipment.containers)
            if total != in_transit + arrived_pending + transferred + skipped:
                raise ValidationError(f"货物 {shipment.shipment_id} 数量不守恒")
            report[shipment.shipment_id] = {
                "registered": total,
                "in_transit": in_transit,
                "arrived_pending": arrived_pending,
                "transferred": transferred,
                "skipped": skipped,
            }
        return report


def _event_order_key(row_or_event: Any) -> tuple[datetime, int, str]:
    if isinstance(row_or_event, RawEvent):
        return (row_or_event.occurred_at, row_or_event.sequence, row_or_event.event_id)
    payload = json.loads(row_or_event["payload_json"])
    return (datetime.fromisoformat(row_or_event["occurred_at"].replace("Z", "+00:00")),
            payload.get("sequence", 0), row_or_event["event_id"])


def _failure_intervals(events: list[RawEvent]) -> list[tuple[str, Interval]]:
    """把同一资源的故障/恢复事件配对成封闭的不可用区间。"""

    by_resource: dict[str, list[RawEvent]] = {}
    for event in events:
        by_resource.setdefault(event.payload["resource_id"], []).append(event)
    intervals: list[tuple[str, Interval]] = []
    for resource_id, items in by_resource.items():
        items.sort(key=lambda item: (item.occurred_at, item.sequence, item.event_id))
        open_start: datetime | None = None
        open_end: datetime | None = None
        for event in items:
            if event.event_type == "equipment.failed":
                end = datetime.fromisoformat(event.payload["end_ts"].replace("Z", "+00:00"))
                if end <= event.occurred_at:
                    raise ValidationError("设备故障结束时间必须晚于发生时间")
                if open_start is None:
                    open_start, open_end = event.occurred_at, end
                else:
                    open_end = max(open_end, end)
            elif open_start is not None:
                close_at = min(open_end, event.occurred_at)
                if close_at > open_start:
                    intervals.append((resource_id, Interval(
                        open_start, close_at, "equipment_failed", resource_id)))
                open_start = open_end = None
        if open_start is not None:
            intervals.append((resource_id, Interval(
                open_start, open_end, "equipment_failed", resource_id)))
    return intervals


def fold(site_id: str, *, cranes: list[str], slots: list[str],
         shipments: dict[str, ShipmentState], batch_planned: dict[str, datetime],
         events: list[RawEvent], holds: dict[str, list[Interval]] | None = None
         ) -> tuple[dict[str, ShipmentState], dict[str, datetime], dict[str, list[Interval]]]:
    """按确定性顺序折叠事件，返回（货物状态，批次实际到达，设备不可用区间）。

    折叠过程强制：未到达不能甩箱后被交接、已交接不能回退为甩箱/未到达、
    交接必须发生在到达之后。违反时间线因果的事件直接拒绝。
    """

    events = sorted(events, key=lambda item: (item.occurred_at, item.sequence, item.event_id))
    batch_of = {sid: state.batch_id for sid, state in shipments.items()}
    batch_arrival: dict[str, datetime] = {}
    equipment_events: list[RawEvent] = []

    # 第一阶段：汇总每个批次“最新一条”到达信息，与消息送达顺序无关。
    latest_arrival_event: dict[str, RawEvent] = {}
    for event in events:
        if event.event_type in ARRIVAL_TYPES:
            batch_id = event.payload["batch_id"]
            if batch_id not in batch_planned:
                raise ValidationError(f"批次 {batch_id} 尚未登记")
            previous = latest_arrival_event.get(batch_id)
            if previous is None or (event.occurred_at, event.sequence, event.event_id) > (
                    previous.occurred_at, previous.sequence, previous.event_id):
                latest_arrival_event[batch_id] = event
    for batch_id, event in latest_arrival_event.items():
        batch_arrival[batch_id] = datetime.fromisoformat(
            event.payload["arrival_at"].replace("Z", "+00:00"))

    # 第二阶段：按业务时序处理甩箱与交接，只强制因果，不强制消息先后。
    for event in events:
        etype = event.event_type
        if etype in ARRIVAL_TYPES:
            continue
        if etype in ("equipment.failed", "equipment.recovered"):
            equipment_events.append(event)
            continue
        if etype == "arrival.skipped":
            batch_id = event.payload["batch_id"]
            arrival = batch_arrival.get(batch_id)
            if arrival is not None and event.occurred_at < arrival:
                raise ValidationError("甩箱时间早于批次实际到达，时间线不一致")
            targets = [box for state in shipments.values() if batch_of[state.shipment_id] == batch_id
                       for cid in event.payload["container_ids"]
                       if (box := state.containers.get(cid)) is not None]
            if len(targets) != len(event.payload["container_ids"]):
                raise ValidationError("甩箱事件引用了不属于该批次的集装箱")
            for box in targets:
                if box.transferred_at is not None and box.transferred_at <= event.occurred_at:
                    raise ValidationError(f"集装箱 {box.container_id} 已交接，不能甩箱")
                box.skipped = True
                box.arrival_at = None
            continue
        if etype == "work.partial":
            state = shipments.get(event.payload["shipment_id"])
            if state is None:
                raise ValidationError("部分完成事件引用了尚未登记的货物")
            arrival = batch_arrival.get(state.batch_id)
            if arrival is None:
                raise ValidationError("交接报告早于批次到达信息，不能记录")
            for cid in event.payload["container_ids"]:
                box = state.containers.get(cid)
                if box is None:
                    raise ValidationError(f"集装箱 {cid} 不属于货物 {state.shipment_id}")
                if box.skipped:
                    raise ValidationError(f"集装箱 {cid} 已甩箱，不能交接")
                if arrival > event.occurred_at:
                    raise ValidationError(f"集装箱 {cid} 的交接时间早于实际到达，时间线不一致")
                if box.transferred_at is None:
                    box.transferred_at = event.occurred_at

    # 到达事实对所有未甩箱箱子生效（含已交接）；一旦到达不可回退为未到达
    for shipment_id, state in shipments.items():
        arrival = batch_arrival.get(batch_of[shipment_id])
        if arrival is None:
            continue
        for box in state.containers.values():
            if not box.skipped and box.arrival_at is None:
                box.arrival_at = arrival

    unavailable: dict[str, list[Interval]] = {}
    for resource_id, interval in _failure_intervals(equipment_events):
        unavailable.setdefault(resource_id, []).append(interval)
    return shipments, batch_arrival, unavailable


def load_projection(connection, site_id: str) -> Projection:
    """从数据库读取注册表、占用与事件，折叠出当前状态。"""

    cranes = [r["resource_id"] for r in connection.execute(
        "SELECT resource_id FROM hub_resources WHERE site_id=? AND kind='crane' ORDER BY resource_id",
        (site_id,))]
    slots = [r["resource_id"] for r in connection.execute(
        "SELECT resource_id FROM hub_resources WHERE site_id=? AND kind='slot' ORDER BY resource_id",
        (site_id,))]

    batch_planned: dict[str, datetime] = {}
    for row in connection.execute(
        "SELECT batch_id, planned_arrival FROM hub_batches WHERE site_id=?", (site_id,)):
        batch_planned[row["batch_id"]] = datetime.fromisoformat(
            row["planned_arrival"].replace("Z", "+00:00"))

    shipments: dict[str, ShipmentState] = {}
    for row in connection.execute(
        "SELECT * FROM hub_shipments WHERE site_id=?", (site_id,)):
            rank_row = connection.execute(
                "SELECT priority_rank FROM hub_contracts WHERE site_id=? AND contract_id=?",
                (site_id, row["contract_id"])).fetchone() if row["contract_id"] else None
            state = ShipmentState(
                row["shipment_id"], row["batch_id"], row["contract_id"],
                rank_row["priority_rank"] if rank_row else None,
                row["inbound_party"], row["outbound_party"], row["duration_minutes"])
            shipments[row["shipment_id"]] = state
            for crow in connection.execute(
                "SELECT container_id, group_id, position FROM hub_containers "
                "WHERE site_id=? AND shipment_id=? ORDER BY position, container_id",
                (site_id, row["shipment_id"])):
                state.containers[crow["container_id"]] = ContainerState(
                    crow["container_id"], row["shipment_id"], crow["group_id"], crow["position"])

    events: list[RawEvent] = []
    for row in connection.execute("SELECT * FROM hub_events WHERE site_id=?", (site_id,)):
        payload = json.loads(row["payload_json"])
        events.append(RawEvent(
            row["event_type"],
            datetime.fromisoformat(row["occurred_at"].replace("Z", "+00:00")),
            row["event_id"], payload.get("sequence", 0), payload))
    events.sort(key=lambda item: (item.occurred_at, item.sequence, item.event_id))

    shipments, batch_arrival, failure_intervals = fold(
        site_id, cranes=cranes, slots=slots, shipments=shipments,
        batch_planned=batch_planned, events=events)

    unavailable: dict[str, list[Interval]] = dict(failure_intervals)
    for row in connection.execute(
        "SELECT resource_id, start_ts, end_ts, reason FROM hub_blockades WHERE site_id=?", (site_id,)):
        start = datetime.fromisoformat(row["start_ts"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(row["end_ts"].replace("Z", "+00:00")) if row["end_ts"] else None
        unavailable.setdefault(row["resource_id"], []).append(
            Interval(start, end, "blockade", row["reason"]))
    for intervals in unavailable.values():
        intervals.sort(key=lambda item: (item.start, item.end or INF))

    holds: dict[str, list[Interval]] = {}
    for row in connection.execute(
        "SELECT resource_id, start_ts, end_ts, shipment_id FROM hub_holds WHERE site_id=?",
        (site_id,)):
        holds.setdefault(row["resource_id"], []).append(Interval(
            datetime.fromisoformat(row["start_ts"].replace("Z", "+00:00")),
            datetime.fromisoformat(row["end_ts"].replace("Z", "+00:00")),
            "hold", row["shipment_id"]))
    for intervals in holds.values():
        intervals.sort(key=lambda item: (item.start, item.end or INF))

    return Projection(site_id, cranes, slots, shipments, batch_planned, batch_arrival,
                      unavailable, holds, len(events))


def merge_intervals(intervals: list[Interval]) -> list[Interval]:
    """合并相交区间，保留最早来源信息用于等待原因诊断。"""

    if not intervals:
        return []
    ordered = sorted(intervals, key=lambda item: (item.start, item.end or INF))
    merged: list[Interval] = []
    for interval in ordered:
        if merged and interval.start < (merged[-1].end or INF):
            last = merged[-1]
            if last.end is None:
                continue
            new_end = max(last.end, interval.end) if interval.end is not None else None
            merged[-1] = Interval(last.start, new_end, last.source, last.ref)
        else:
            merged.append(interval)
    return merged


def earliest_after(busy: list[Interval], start: datetime, duration: timedelta) -> datetime | None:
    """在忙碌区间集合上寻找不早于 start 的可行空档起点；永久占用返回 None。"""

    current = start
    for _ in range(MAX_SCHEDULE_STEPS):
        window_end = current + duration
        for interval in busy:
            if interval.end is not None and interval.end <= current:
                continue
            if interval.start >= window_end:
                continue
            if interval.end is None:
                return None
            current = interval.end
            break
        else:
            return current
    return None


def earliest_pair_slot(projection: Projection, ready: datetime, duration: timedelta,
                        crane_id: str, slot_id: str,
                        extra_busy: dict[str, list[Interval]] | None = None) -> datetime | None:
    busy = merge_intervals(
        projection.holds.get(crane_id, []) + projection.unavailable.get(crane_id, [])
        + (extra_busy or {}).get(crane_id, []))
    busy += merge_intervals(
        projection.holds.get(slot_id, []) + projection.unavailable.get(slot_id, [])
        + (extra_busy or {}).get(slot_id, []))
    return earliest_after(merge_intervals(busy), ready, duration)


@dataclass(frozen=True)
class GroupTask:
    shipment: ShipmentState
    group_id: str
    container_ids: tuple[str, ...]
    ready_at: datetime


def pending_group_tasks(projection: Projection) -> list[GroupTask]:
    """推导仍需安排的箱组任务：未甩箱、未交接；按就绪时间与优先合同排序。"""

    tasks: list[GroupTask] = []
    for shipment in projection.shipments.values():
        groups: dict[str, list[ContainerState]] = {}
        for box in shipment.containers.values():
            if not box.skipped and box.transferred_at is None:
                groups.setdefault(box.group_id, []).append(box)
        for group_id, boxes in groups.items():
            boxes.sort(key=lambda item: (item.position, item.container_id))
            arrivals = [box.arrival_at for box in boxes if box.arrival_at is not None]
            if arrivals:
                ready = max(arrivals)
            else:
                ready = projection.batch_planned[shipment.batch_id]
            tasks.append(GroupTask(
                shipment, group_id, tuple(b.container_id for b in boxes), ready))
    tasks.sort(key=lambda task: (
        task.ready_at,
        task.shipment.priority_rank if task.shipment.priority_rank is not None else LARGE_RANK,
        task.shipment.shipment_id,
        task.group_id,
    ))
    return tasks


def schedule_tasks(projection: Projection, tasks: list[GroupTask],
                   reserved: dict[str, list[Interval]] | None = None) -> list[dict[str, Any]]:
    """贪心列表排程：每任务取所有吊机×堆位组合中最早的可行空档。

    reserved 记录本方案内已放置的占用，使同一吊机与堆位不会在同一
    方案中被重复承诺。
    """

    reserved = reserved if reserved is not None else {}
    scheduled: list[dict[str, Any]] = []
    for task in tasks:
        duration = timedelta(minutes=task.shipment.duration_minutes)
        best: tuple[datetime, str, str] | None = None
        for crane_id in projection.cranes:
            for slot_id in projection.slots:
                start = earliest_pair_slot(projection, task.ready_at, duration,
                                           crane_id, slot_id, reserved)
                if start is not None and (best is None or (start, crane_id, slot_id) < best):
                    best = (start, crane_id, slot_id)
        if best is None:
            raise ValidationError(
                f"货物 {task.shipment.shipment_id} 箱组 {task.group_id} 暂无可安排的吊机或堆位")
        start, crane_id, slot_id = best
        end = start + duration
        for resource_id in (crane_id, slot_id):
            reserved.setdefault(resource_id, []).append(
                Interval(start, end, "plan", task.shipment.shipment_id))
        scheduled.append({
            "shipment_id": task.shipment.shipment_id,
            "group_key": task.group_id,
            "container_ids": list(task.container_ids),
            "ready_at": format_ts(task.ready_at),
            "start_ts": format_ts(start),
            "end_ts": format_ts(end),
            "crane_id": crane_id,
            "slot_id": slot_id,
        })
    scheduled.sort(key=lambda item: (item["start_ts"], item["shipment_id"], item["group_key"]))
    for index, item in enumerate(scheduled):
        item["task_index"] = index
    return scheduled


def _active_intervals(intervals: list[Interval], moment: datetime,
                      duration: timedelta) -> list[Interval]:
    window_end = moment + duration
    return [item for item in intervals
            if item.start < window_end and (item.end is None or item.end > moment)]


def diagnose_waiting(projection: Projection, task: GroupTask, now: datetime) -> str | None:
    """区分关键等待原因：未到达、设备故障、封锁窗口、资源争抢。"""

    if task.ready_at > now:
        return "waiting_arrival"
    duration = timedelta(minutes=task.shipment.duration_minutes)
    available_pair = False
    sees_failure = False
    sees_blockade = False
    for crane_id in projection.cranes:
        for slot_id in projection.slots:
            blocked = (_active_intervals(projection.unavailable.get(crane_id, []), now, duration)
                       + _active_intervals(projection.unavailable.get(slot_id, []), now, duration))
            if blocked:
                sources = {item.source for item in blocked}
                sees_failure = sees_failure or "equipment_failed" in sources
                sees_blockade = sees_blockade or "blockade" in sources
            else:
                available_pair = True
    if not available_pair:
        return "equipment_failed" if sees_failure and not sees_blockade else "blockade_window"
    earliest = min(
        (start for crane_id in projection.cranes for slot_id in projection.slots
         if (start := earliest_pair_slot(projection, task.ready_at, duration,
                                         crane_id, slot_id)) is not None),
        default=None)
    if earliest is not None and earliest > now:
        return "resource_contention"
    return None
