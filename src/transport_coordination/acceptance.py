"""运行基础服务与换装协同的离线端到端验收。

验收使用 ManualClock 精确控制时间，覆盖：
- 跨日有期限方案与三方确认后的原子占用；
- 提前、延误、甩箱、设备故障只推动受影响任务重排；
- 已交接货物不回退、数量守恒；
- 相同事件重放返回原结果、乱序消息不改变口径；
- 冻结口径下的一小时换装率。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock, ManualClock
from .errors import ConflictError, PermissionDenied
from .hub_service import HubService
from .service import DomainService
from .storage import Database


def _base_flow(database: Database) -> dict[str, object]:
    service = DomainService(database, FixedClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)))
    service.register_organization(request_id="req-org", actor_id="bootstrap",
                                  organization_id="org-001", name="示范运营机构")
    service.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-001",
                           display_name="系统管理员", role="admin", organization_id="org-001")
    service.register_actor(request_id="req-operator", actor_id="admin-001", new_actor_id="operator-001",
                           display_name="运营负责人", role="operator", organization_id="org-001")
    service.register_site(request_id="req-site", actor_id="operator-001", site_id="site-001",
                          organization_id="org-001", name="一号交通节点", timezone_name="Asia/Shanghai")
    first = service.record_domain_data(request_id="req-data", actor_id="operator-001", site_id="site-001",
                                       category="operator_profile", external_key="record-001",
                                       data={"name": "基础资料", "enabled": True})
    replay = service.record_domain_data(request_id="req-data", actor_id="operator-001", site_id="site-001",
                                        category="operator_profile", external_key="record-001",
                                        data={"name": "基础资料", "enabled": True})
    return {"first_replayed": first.replayed, "second_replayed": replay.replayed}


def _confirm_and_commit(hub: HubService, plan_id: str, *, req_prefix: str) -> None:
    for index, actor in enumerate(("op-rail", "op-port", "op-truck")):
        try:
            hub.confirm_plan(request_id=f"{req_prefix}-cf-{index}-{plan_id[:8]}",
                             actor_id=actor, plan_id=plan_id)
        except PermissionDenied:
            continue  # 重排方案可能只涉及部分承运方
    hub.commit_plan(request_id=f"{req_prefix}-cm-{plan_id[:8]}",
                    actor_id="admin-hub", plan_id=plan_id)


def _hub_flow(database: Database) -> dict[str, object]:
    clock = ManualClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc))
    domain = DomainService(database, clock)
    hub = HubService(database, clock)

    # 枢纽运营机构与铁路、港区、集疏运三个承运方
    domain.register_organization(request_id="h-org-hub", actor_id="bootstrap",
                                 organization_id="hub-001", name="枢纽运营中心")
    domain.register_actor(request_id="h-admin", actor_id="bootstrap", new_actor_id="admin-hub",
                          display_name="枢纽管理员", role="admin", organization_id="hub-001")
    for req, org, actor, name in (
            ("h-org-rail", "rail-001", "op-rail", "铁路值班员"),
            ("h-org-port", "port-001", "op-port", "港区值班员"),
            ("h-org-truck", "truck-001", "op-truck", "车队值班员")):
        domain.register_organization(request_id=req, actor_id="admin-hub",
                                     organization_id=org, name=name)
        domain.register_actor(request_id=f"{req}-actor", actor_id="admin-hub", new_actor_id=actor,
                              display_name=name, role="operator", organization_id=org)
    domain.register_site(request_id="h-site", actor_id="admin-hub", site_id="hub-site",
                         organization_id="hub-001", name="重点货运枢纽",
                         timezone_name="Asia/Shanghai")

    # 两台吊机、两个堆位；优先合同；跨日的列车/船舶/车辆批次
    for req, rid, kind, name in (
            ("h-c1", "crane-1", "crane", "一号吊机"),
            ("h-c2", "crane-2", "crane", "二号吊机"),
            ("h-t1", "slot-1", "slot", "一号堆位"),
            ("h-t2", "slot-2", "slot", "二号堆位")):
        hub.register_resource(request_id=req, actor_id="admin-hub", site_id="hub-site",
                              resource_id=rid, kind=kind, name=name)
    hub.register_contract(request_id="h-contract", actor_id="admin-hub", site_id="hub-site",
                          contract_id="p1", title="优先合同 P1", priority_rank=0)
    hub.register_batch(request_id="h-rail", actor_id="admin-hub", site_id="hub-site",
                       batch_id="rail-1", mode="rail", planned_arrival="2026-09-26T01:00:00Z")
    hub.register_batch(request_id="h-vessel", actor_id="admin-hub", site_id="hub-site",
                       batch_id="vessel-1", mode="vessel", planned_arrival="2026-09-26T02:00:00Z")
    hub.register_batch(request_id="h-truck", actor_id="admin-hub", site_id="hub-site",
                       batch_id="truck-1", mode="vehicle", planned_arrival="2026-09-26T03:00:00Z")

    hub.register_shipment(
        request_id="h-sh1", actor_id="admin-hub", site_id="hub-site", shipment_id="sh-1",
        batch_id="rail-1", contract_id="p1", inbound_party="rail-001",
        outbound_party="port-001", duration_minutes=30,
        containers=[{"container_id": "box-1", "group_id": "g1", "position": 0},
                    {"container_id": "box-2", "group_id": "g1", "position": 1}])
    hub.register_shipment(
        request_id="h-sh2", actor_id="admin-hub", site_id="hub-site", shipment_id="sh-2",
        batch_id="vessel-1", inbound_party="port-001", outbound_party="truck-001",
        duration_minutes=30,
        containers=[{"container_id": "box-3", "group_id": "g1", "position": 0}])
    hub.register_shipment(
        request_id="h-sh3", actor_id="admin-hub", site_id="hub-site", shipment_id="sh-3",
        batch_id="rail-1", inbound_party="rail-001", outbound_party="truck-001",
        duration_minutes=45,
        containers=[{"container_id": "box-4", "group_id": "g1", "position": 0},
                    {"container_id": "box-5", "group_id": "g1", "position": 1}])

    # crane-1 在 01:00-01:40 封锁（夜间检修窗口）
    hub.register_blockade(request_id="h-block", actor_id="admin-hub", site_id="hub-site",
                          resource_id="crane-1", start_ts="2026-09-26T01:00:00Z",
                          end_ts="2026-09-26T01:40:00Z", reason="夜间检修")

    # 提案在到达时刻之前：按静态时刻也能看到吊机/堆位不会重复承诺
    draft = hub.create_plan(request_id="h-plan-draft", actor_id="admin-hub",
                            site_id="hub-site")
    assert draft["state"] == "open"
    windows = {(t["crane_id"], t["slot_id"], t["start_ts"]) for t in draft["tasks"]}
    assert len(windows) == len(draft["tasks"]), "方案内吊机与堆位被重复承诺"

    # 提案有期限：时钟跨过到期点后确认必须失败
    clock.advance(minutes=31)
    try:
        hub.confirm_plan(request_id="h-lapsed", actor_id="op-rail",
                         plan_id=draft["plan_id"])
        raise AssertionError("过期方案不应可确认")
    except ConflictError:
        pass

    # 覆盖跨日的新提案，三方确认后原子占用
    plan = hub.create_plan(request_id="h-plan-1", actor_id="admin-hub",
                           site_id="hub-site", valid_minutes=1440)
    _confirm_and_commit(hub, plan["plan_id"], req_prefix="p1")
    committed = hub.get_plan(plan["plan_id"])
    assert committed["state"] == "committed"

    # 列车提前到 00:30：只重排 rail-1 的两个任务，sh-2 不动
    early = hub.ingest_event(
        request_id="h-ev-early", actor_id="op-rail", site_id="hub-site",
        event={"event_id": "ev-rail-early", "event_type": "arrival.early",
               "occurred_at": "2026-09-25T20:00:00Z", "sequence": 1,
               "batch_id": "rail-1", "arrival_at": "2026-09-26T00:30:00Z"})
    assert len(early["released_tasks"]) == 2
    plan2 = hub.get_plan(early["rescheduled_plan_id"])
    assert plan2["state"] == "open"
    _confirm_and_commit(hub, plan2["plan_id"], req_prefix="p2")

    # 船舶延误到 02:20：只重排 sh-2
    delayed = hub.ingest_event(
        request_id="h-ev-delay", actor_id="op-port", site_id="hub-site",
        event={"event_id": "ev-vessel-delay", "event_type": "arrival.delayed",
               "occurred_at": "2026-09-25T22:00:00Z", "sequence": 1,
               "batch_id": "vessel-1", "arrival_at": "2026-09-26T02:20:00Z"})
    assert [t["shipment_id"] for t in hub.get_plan(delayed["rescheduled_plan_id"])["tasks"]] == ["sh-2"]
    _confirm_and_commit(hub, delayed["rescheduled_plan_id"], req_prefix="p3")

    # box-5 甩箱：sh-3 的箱组收缩并重排
    skipped = hub.ingest_event(
        request_id="h-ev-skip", actor_id="op-rail", site_id="hub-site",
        event={"event_id": "ev-skip-5", "event_type": "arrival.skipped",
               "occurred_at": "2026-09-26T00:31:00Z", "sequence": 2,
               "batch_id": "rail-1", "container_ids": ["box-5"], "reason": "箱体破损"})
    skip_plan = hub.get_plan(skipped["rescheduled_plan_id"])
    assert skip_plan["tasks"][0]["container_ids"] == ["box-4"]
    _confirm_and_commit(hub, skip_plan["plan_id"], req_prefix="p4")

    # 设备故障 00:40-01:20：只释放与故障窗口重叠且未交接的任务
    failed = hub.ingest_event(
        request_id="h-ev-fail", actor_id="admin-hub", site_id="hub-site",
        event={"event_id": "ev-crane2-fail", "event_type": "equipment.failed",
               "occurred_at": "2026-09-26T00:40:00Z", "sequence": 3,
               "resource_id": "crane-2", "end_ts": "2026-09-26T01:20:00Z"})
    fail_plan = hub.get_plan(failed["rescheduled_plan_id"])
    affected = sorted(t["shipment_id"] for t in fail_plan["tasks"])
    assert affected == ["sh-3"]
    _confirm_and_commit(hub, fail_plan["plan_id"], req_prefix="p5")

    # sh-1 在 00:55 完成部分交接（到达后 25 分钟）
    partial = hub.ingest_event(
        request_id="h-ev-partial", actor_id="op-port", site_id="hub-site",
        event={"event_id": "ev-work-1", "event_type": "work.partial",
               "occurred_at": "2026-09-26T00:55:00Z", "sequence": 4,
               "shipment_id": "sh-1", "container_ids": ["box-1", "box-2"]})
    assert partial["rescheduled_plan_id"] is None
    shipment1 = hub.get_shipment("hub-site", "sh-1")
    assert shipment1["handover_stage"] == "completed"
    assert shipment1["custodian"] == "port-001"
    assert shipment1["within_one_hour"] is True

    # 已交接不能回退：晚到的更早预测不抢占最新到达，也不重排已完成任务
    stale = hub.ingest_event(
        request_id="h-ev-stale", actor_id="op-rail", site_id="hub-site",
        event={"event_id": "ev-rail-plan", "event_type": "arrival.confirmed",
               "occurred_at": "2026-09-25T19:00:00Z", "sequence": 0,
               "batch_id": "rail-1", "arrival_at": "2026-09-26T01:00:00Z"})
    assert stale["released_tasks"] == [] and stale["rescheduled_plan_id"] is None
    assert hub.get_shipment("hub-site", "sh-1")["transferred"] == ["box-1", "box-2"]

    # 相同事件重放：换 request_id 也返回同一结果，不新增交接
    replay = hub.ingest_event(
        request_id="h-ev-partial-replay", actor_id="op-port", site_id="hub-site",
        event={"event_id": "ev-work-1", "event_type": "work.partial",
               "occurred_at": "2026-09-26T00:55:00Z", "sequence": 4,
               "shipment_id": "sh-1", "container_ids": ["box-1", "box-2"]})
    assert replay["replayed"] is True and replay["rescheduled_plan_id"] is None

    # 乱序送达船舶消息：先发“延误”再补发更早的“确认”，口径保持 02:20
    rate_after_delay = hub.compute_rate("hub-site", "2026-09-26T03:30:00Z")
    out_of_order = hub.ingest_event(
        request_id="h-ev-vessel-confirm", actor_id="op-port", site_id="hub-site",
        event={"event_id": "ev-vessel-confirm", "event_type": "arrival.confirmed",
               "occurred_at": "2026-09-25T21:00:00Z", "sequence": 0,
               "batch_id": "vessel-1", "arrival_at": "2026-09-26T02:00:00Z"})
    assert out_of_order["released_tasks"] == []
    rate_after_ooo = hub.compute_rate("hub-site", "2026-09-26T03:30:00Z")
    assert rate_after_delay == rate_after_ooo

    # 关键等待原因：00:20 时船舶要 02:20 才到，sh-2 正在等待到达
    clock.set(datetime(2026, 9, 26, 0, 20, tzinfo=timezone.utc))
    assert hub.get_shipment("hub-site", "sh-2")["waiting_reason"] == "waiting_arrival"

    # sh-2 在 03:00 交接（40 分钟），sh-3 的 box-4 在 02:05 交接（95 分钟）
    hub.ingest_event(
        request_id="h-ev-work2", actor_id="op-truck", site_id="hub-site",
        event={"event_id": "ev-work-2", "event_type": "work.partial",
               "occurred_at": "2026-09-26T03:00:00Z", "sequence": 5,
               "shipment_id": "sh-2", "container_ids": ["box-3"]})
    hub.ingest_event(
        request_id="h-ev-work3", actor_id="op-truck", site_id="hub-site",
        event={"event_id": "ev-work-3", "event_type": "work.partial",
               "occurred_at": "2026-09-26T02:05:00Z", "sequence": 6,
               "shipment_id": "sh-3", "container_ids": ["box-4"]})

    # 数量守恒：登记 5 = 在途 0 + 已交接 4 + 甩箱 1
    conservation = hub.conservation("hub-site")["shipments"]
    totals = {"registered": 0, "transferred": 0, "skipped": 0, "in_transit": 0}
    for row in conservation.values():
        for key in totals:
            totals[key] += row[key]
    assert totals == {"registered": 5, "in_transit": 0, "transferred": 4, "skipped": 1}

    # 冻结口径：4 箱到达，3 箱一小时内完成 => 0.75；同一时点不可二次冻结
    rate = hub.compute_rate("hub-site", "2026-09-26T03:30:00Z")
    assert (rate["arrived_count"], rate["within_60m_count"], rate["rate"]) == (4, 3, 0.75)
    freeze = hub.freeze_rate(request_id="h-freeze", actor_id="admin-hub",
                             site_id="hub-site", as_of="2026-09-26T03:30:00Z")
    try:
        hub.freeze_rate(request_id="h-freeze-again", actor_id="admin-hub",
                        site_id="hub-site", as_of="2026-09-26T03:30:00Z")
        raise AssertionError("同一时点不应允许重复冻结")
    except ConflictError:
        pass

    # 关键等待原因已在交接事件前核对
    timeline = hub.timeline("hub-site")
    ordered_keys = sorted(
        (e["occurred_at"], e["payload"].get("sequence", 0), e["event_id"]) for e in timeline)
    actual_keys = [
        (e["occurred_at"], e["payload"].get("sequence", 0), e["event_id"]) for e in timeline]
    assert actual_keys == ordered_keys and len(timeline) >= 8

    return {
        "hub_plans_committed": 5,
        "hub_rate": rate["rate"],
        "hub_freeze_id": freeze["freeze_id"],
        "hub_conservation": totals,
        "hub_timeline_events": len(timeline),
        "hub_replay_same_result": replay["replayed"] is True,
        "hub_out_of_order_stable": rate_after_delay == rate_after_ooo,
    }


def run() -> dict[str, object]:
    """执行完整登记链与换装协同剧本并返回结果。"""

    with tempfile.TemporaryDirectory() as directory:
        base_db = Database(Path(directory) / "acceptance.sqlite3")
        base = _base_flow(base_db)
        base_db.close()

        hub_db = Database(Path(directory) / "hub.sqlite3")
        hub = _hub_flow(hub_db)
        valid, event_count = DomainService(hub_db, FixedClock(
            datetime(2026, 9, 25, tzinfo=timezone.utc))).verify_audit()
        hub_db.close()

        return {
            "status": "ok" if valid else "audit_invalid",
            "records": 1,
            "audit_events": event_count,
            "audit_valid": valid,
            **base,
            **hub,
        }


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
