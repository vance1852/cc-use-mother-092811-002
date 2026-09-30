"""换装协同的离线端到端验收：可控时钟、跨日计划、故障重排、冻结考核与关库恢复。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import MutableClock
from .hub import HubService
from .service import DomainService
from .storage import Database


def _bootstrap(database, clock):
    svc = DomainService(database, clock)
    hub = HubService(database, clock)
    svc.register_organization(request_id="org-hub", actor_id="bootstrap",
                              organization_id="HUB", name="枢纽运营方")
    svc.register_actor(request_id="mgr", actor_id="bootstrap", new_actor_id="mgr",
                       display_name="枢纽经理", role="admin", organization_id="HUB")
    svc.register_organization(request_id="org-rail", actor_id="mgr",
                              organization_id="RAIL", name="铁路公司")
    svc.register_organization(request_id="org-road", actor_id="mgr",
                              organization_id="ROAD", name="集卡公司")
    svc.register_actor(request_id="railop", actor_id="mgr", new_actor_id="railop",
                       display_name="铁路值班", role="operator", organization_id="RAIL")
    svc.register_actor(request_id="roadop", actor_id="mgr", new_actor_id="roadop",
                       display_name="集卡值班", role="operator", organization_id="ROAD")
    svc.register_site(request_id="site", actor_id="mgr", site_id="S1", organization_id="HUB",
                      name="重点货运枢纽", timezone_name="Asia/Shanghai")
    for rid, kind, name in (("CR1", "crane", "一号吊机"), ("CR2", "crane", "二号吊机"),
                            ("SLOT1", "slot", "甲堆位"), ("SLOT2", "slot", "乙堆位")):
        hub.register_resource(request_id=f"res-{rid}", actor_id="mgr", resource_id=rid,
                              site_id="S1", kind=kind, name=name)
    hub.register_contract(request_id="pc", actor_id="mgr", contract_id="PC1", site_id="S1",
                          priority=0, name="快运合同")
    return svc, hub


def run() -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "hub_acceptance.sqlite3"
        clock = MutableClock(datetime(2026, 9, 29, 23, 10, tzinfo=timezone.utc))
        database = Database(db_path)
        svc, hub = _bootstrap(database, clock)

        hub.register_shipment(
            request_id="ship", actor_id="mgr", shipment_id="SHIP1", site_id="S1",
            inbound_party="RAIL", outbound_party="ROAD", contract_id="PC1",
            groups=[{"group_id": "G1", "quantity": 10, "work_minutes": 20},
                    {"group_id": "G2", "quantity": 8, "work_minutes": 20}])
        # 列车 23:20 到达（跨午夜），集卡次日 01:30 出发。
        hub.register_batch(request_id="bin", actor_id="mgr", batch_id="TRAIN1", site_id="S1",
                           mode="rail", direction="inbound",
                           planned_arrival="2026-09-29T23:20Z",
                           manifest=[{"group_id": "G1", "quantity": 10},
                                     {"group_id": "G2", "quantity": 8}])
        hub.register_batch(request_id="bout", actor_id="mgr", batch_id="TRUCK1", site_id="S1",
                           mode="road", direction="outbound",
                           planned_arrival="2026-09-30T01:00Z",
                           planned_departure="2026-09-30T01:30Z",
                           manifest=[{"group_id": "G1", "quantity": 10},
                                     {"group_id": "G2", "quantity": 8}])
        # 列车提前到 23:15，两箱组全部到达并完成进站交接。
        hub.ingest_event(request_id="arr", actor_id="railop", event_id="E-ARR",
                         event_type="arrival", occurred_at="2026-09-29T23:15Z",
                         payload={"site_id": "S1", "batch_id": "TRAIN1",
                                  "actual_arrival": "2026-09-29T23:15Z"})
        for gid, qty in (("G1", 10), ("G2", 8)):
            hub.ingest_event(request_id=f"aq-{gid}", actor_id="railop",
                             event_id=f"E-AQ-{gid}", event_type="arrival_qty",
                             occurred_at="2026-09-29T23:16Z",
                             payload={"site_id": "S1", "batch_id": "TRAIN1",
                                      "group_id": gid, "quantity": qty})
            hub.ingest_event(request_id=f"hi-{gid}", actor_id="railop",
                             event_id=f"E-HI-{gid}", event_type="handover",
                             occurred_at="2026-09-29T23:18Z",
                             payload={"site_id": "S1", "shipment_id": "SHIP1",
                                      "group_id": gid, "stage": "inbound",
                                      "quantity": qty, "at_ts": "2026-09-29T23:18Z"})

        plan = hub.create_plan(request_id="plan", actor_id="mgr", site_id="S1",
                               horizon_start="2026-09-29T23:10Z",
                               horizon_end="2026-09-30T02:30Z")["plan"]
        hub.confirm_plan(request_id="cf-r", actor_id="railop", plan_id=plan["plan_id"],
                         party="RAIL")
        committed = hub.confirm_plan(request_id="cf-d", actor_id="roadop",
                                     plan_id=plan["plan_id"], party="ROAD")
        committed_occupancies = len(committed["confirmation"]["occupancies"])
        assert committed["confirmation"]["status"] == "confirmed"

        allocation = {a["group_id"]: a for a in plan["allocation"]}
        # 23:20 通报 CR1 故障至 23:50，只重排命中的任务。
        clock.set(datetime(2026, 9, 29, 23, 20, tzinfo=timezone.utc))
        faulted_group = next(
            gid for gid, a in allocation.items()
            if a["crane"] == "CR1" and a["crane_start"] < "2026-09-29T23:50Z")
        hub.ingest_event(request_id="fault", actor_id="mgr", event_id="E-FAULT",
                         event_type="equipment_fault", occurred_at="2026-09-29T23:20Z",
                         payload={"site_id": "S1", "resource_id": "CR1",
                                  "start_ts": allocation[faulted_group]["crane_start"],
                                  "end_ts": "2026-09-29T23:50Z", "reason": "吊机故障"})
        tasks_after_fault = {t["group_id"]: t["status"]
                             for t in hub.list_tasks("S1")["tasks"]}
        held_keys_before = sorted(
            (e["resource_id"], e["start_ts"], e["end_ts"], e["status"])
            for e in hub.get_timeline("S1")["events"] if e["kind"] == "occupancy")

        # G1 在一小时内完成出站交接；跨午夜作业。
        hub.ingest_event(request_id="ho-g1", actor_id="roadop", event_id="E-HO-G1",
                         event_type="handover", occurred_at="2026-09-29T23:55Z",
                         payload={"site_id": "S1", "shipment_id": "SHIP1", "group_id": "G1",
                                  "stage": "outbound", "quantity": 10,
                                  "at_ts": "2026-09-29T23:55Z"})
        report = hub.freeze_kpi(
            request_id="kpi", actor_id="mgr", site_id="S1",
            window_start="2026-09-29T23:00Z", window_end="2026-09-30T02:00Z")["report"]
        audit_before = svc.verify_audit()
        tasks_before_close = {t["group_id"]: t["status"]
                              for t in hub.list_tasks("S1")["tasks"]}

        # ---- 关库重开：派生状态由事件折叠重建，冻结报告原样保留 ----
        database.close()
        database = Database(db_path)
        svc2 = DomainService(database, clock)
        hub2 = HubService(database, clock)
        audit_after = svc2.verify_audit()
        tasks_after_reopen = {t["group_id"]: t["status"]
                              for t in hub2.list_tasks("S1")["tasks"]}
        report2 = hub2.get_kpi_report(report["report_id"])
        held_keys_after = sorted(
            (e["resource_id"], e["start_ts"], e["end_ts"], e["status"])
            for e in hub2.get_timeline("S1")["events"] if e["kind"] == "occupancy")
        held_after_reopen = sum(1 for k in held_keys_after if k[3] == "held")
        database.close()

        checks = {
            "committed_occupancies": committed_occupancies,
            "faulted_group": faulted_group,
            "tasks_after_fault": tasks_after_fault,
            "tasks_after_reopen": tasks_after_reopen,
            "occupancies_preserved": held_keys_before == held_keys_after,
            "kpi_total_units": report["total_units"],
            "kpi_on_time_units": report["on_time_units"],
            "kpi_after_reopen_on_time": report2["on_time_units"],
            "held_after_reopen": held_after_reopen,
            "audit_before_valid": audit_before[0],
            "audit_after_valid": audit_after[0],
            "audit_events": audit_after[1],
        }
        ok = (
            committed_occupancies == 4
            and tasks_after_fault[faulted_group] == "pending"
            and tasks_after_reopen == tasks_before_close
            and held_keys_before == held_keys_after
            and report["total_units"] == 18
            and report["on_time_units"] == 10
            and report2["on_time_units"] == 10
            and held_after_reopen >= 2
            and audit_before[0] and audit_after[0]
        )
        return {"status": "ok" if ok else "failed", **checks}


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
