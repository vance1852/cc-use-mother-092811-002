"""多式联运转装协同的数据对象。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class HubResource:
    """枢纽内可占用的作业资源：吊机或堆位。"""

    site_id: str
    resource_id: str
    kind: str
    name: str


@dataclass(frozen=True)
class PriorityContract:
    """优先合同，rank 越小优先级越高。"""

    site_id: str
    contract_id: str
    title: str
    priority_rank: int


@dataclass(frozen=True)
class ArrivalBatch:
    """承运方按列车、船舶或集疏运车辆组织的批次到达。"""

    site_id: str
    batch_id: str
    mode: str
    planned_arrival: str


@dataclass(frozen=True)
class ContainerSpec:
    """箱组内的单个集装箱；position 决定甩箱与交接的箱内次序。"""

    container_id: str
    group_id: str
    position: int


@dataclass(frozen=True)
class Shipment:
    """一票需要在两种运输方式之间换装的货物。"""

    site_id: str
    shipment_id: str
    batch_id: str
    contract_id: str | None
    inbound_party: str
    outbound_party: str
    duration_minutes: int
    containers: tuple[ContainerSpec, ...]


@dataclass(frozen=True)
class TimelineEvent:
    """进入统一时间线的领域事件，按业务时间与序号确定。"""

    event_id: str
    event_type: str
    occurred_at: str
    payload: dict[str, Any]

    @property
    def key(self) -> tuple[str, int, str]:
        return (self.occurred_at, self.payload.get("sequence", 0), self.event_id)


@dataclass(frozen=True)
class ScheduledTask:
    """方案中对一个箱组作业的原子吊机+堆位预约。"""

    task_index: int
    shipment_id: str
    group_key: str
    container_ids: tuple[str, ...]
    ready_at: str
    start_ts: str
    end_ts: str
    crane_id: str
    slot_id: str


@dataclass(frozen=True)
class PlanView:
    """对外暴露的换装方案视图。"""

    plan_id: str
    site_id: str
    state: str
    expires_at: str
    committed_at: str | None
    supersedes: str | None
    confirmations: tuple[str, ...]
    tasks: tuple[ScheduledTask, ...]


@dataclass(frozen=True)
class ShipmentView:
    """一票货物的交接责任、进度与等待原因。"""

    shipment_id: str
    batch_id: str
    contract_id: str | None
    inbound_party: str
    outbound_party: str
    container_ids: tuple[str, ...]
    arrived: tuple[str, ...]
    transferred: tuple[str, ...]
    handover_stage: str
    custodian: str
    waiting_reason: str | None
    plan_id: str | None
    scheduled_start: str | None
    scheduled_end: str | None
    within_one_hour: bool | None


@dataclass(frozen=True)
class RateFreeze:
    """冻结口径下的一小时换装率。"""

    freeze_id: str
    site_id: str
    as_of: str
    arrived_count: int
    transferred_count: int
    within_60m_count: int
    rate: float
