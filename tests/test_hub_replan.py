"""故障、延误、甩箱驱动的受影响重排，以及冻结考核口径测试。"""

import unittest

from transport_coordination.errors import ConflictError

from hub_support import build_hub


def two_group_world(hub, arrived=("G1", "G2"), handed=("G1", "G2")):
    """建两箱组世界；arrived/handed 控制哪些箱组已报到达量、已进站交接。"""
    hub.register_shipment(
        request_id="ship", actor_id="mgr", shipment_id="SHIP1", site_id="S1",
        inbound_party="RAIL", outbound_party="ROAD",
        groups=[{"group_id": "G1", "quantity": 10, "work_minutes": 20},
                {"group_id": "G2", "quantity": 10, "work_minutes": 20}])
    hub.register_batch(
        request_id="bin", actor_id="mgr", batch_id="TRAIN1", site_id="S1",
        mode="rail", direction="inbound", planned_arrival="2026-09-29T23:20Z",
        manifest=[{"group_id": "G1", "quantity": 10}, {"group_id": "G2", "quantity": 10}])
    hub.register_batch(
        request_id="bout", actor_id="mgr", batch_id="TRUCK1", site_id="S1",
        mode="road", direction="outbound",
        planned_arrival="2026-09-30T01:00Z", planned_departure="2026-09-30T01:30Z",
        manifest=[{"group_id": "G1", "quantity": 10}, {"group_id": "G2", "quantity": 10}])
    hub.ingest_event(request_id="arr", actor_id="railop", event_id="E-ARR",
                     event_type="arrival", occurred_at="2026-09-29T23:20Z",
                     payload={"site_id": "S1", "batch_id": "TRAIN1",
                              "actual_arrival": "2026-09-29T23:20Z"})
    for gid in arrived:
        hub.ingest_event(request_id=f"aq-{gid}", actor_id="railop", event_id=f"E-AQ-{gid}",
                         event_type="arrival_qty", occurred_at="2026-09-29T23:21Z",
                         payload={"site_id": "S1", "batch_id": "TRAIN1", "group_id": gid,
                                  "quantity": 10})
    for gid in handed:
        hub.ingest_event(request_id=f"hi-{gid}", actor_id="railop", event_id=f"E-HI-{gid}",
                         event_type="handover", occurred_at="2026-09-29T23:25Z",
                         payload={"site_id": "S1", "shipment_id": "SHIP1", "group_id": gid,
                                  "stage": "inbound", "quantity": 10,
                                  "at_ts": "2026-09-29T23:25Z"})


def confirm_all(hub, plan_id):
    hub.confirm_plan(request_id=f"c-r-{plan_id}", actor_id="railop",
                     plan_id=plan_id, party="RAIL")
    return hub.confirm_plan(request_id=f"c-d-{plan_id}", actor_id="roadop",
                            plan_id=plan_id, party="ROAD")


def plan_now(hub, req="plan"):
    return hub.create_plan(request_id=req, actor_id="mgr", site_id="S1",
                           horizon_start="2026-09-29T23:00Z",
                           horizon_end="2026-09-30T03:00Z")


def allocation_of(hub, plan_id):
    row = hub.database.connection.execute(
        "SELECT allocation_json FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
    import json
    return {a["group_id"]: a for a in json.loads(row["allocation_json"])}


def occ_states(hub, group_id):
    events = hub.get_timeline("S1")["events"]
    return [(e["resource_id"], e["status"], e["release_reason"])
            for e in events if e["kind"] == "occupancy" and e["group_id"] == group_id]


def task(hub, gid):
    return next(t for t in hub.list_tasks("S1")["tasks"] if t["group_id"] == gid)


class ReplanTest(unittest.TestCase):
    def setUp(self):
        self.database, self.svc, self.hub, self.clock = build_hub()
        # G1 已进站交接；G2 已到达但尚未进站交接，以便覆盖延误、甩箱两类情形。
        two_group_world(self.hub, handed=("G1",))

    def tearDown(self):
        self.database.close()

    def test_equipment_fault_replans_only_affected_task(self):
        plan = plan_now(self.hub)["plan"]
        alloc = allocation_of(self.hub, plan["plan_id"])
        confirm_all(self.hub, plan["plan_id"])
        target_gid = "G1"
        crane = alloc[target_gid]["crane"]
        self.clock.advance(minutes=1)
        self.hub.ingest_event(
            request_id="fault", actor_id="mgr", event_id="E-FAULT",
            event_type="equipment_fault", occurred_at="2026-09-29T23:22Z",
            payload={"site_id": "S1", "resource_id": crane,
                     "start_ts": alloc[target_gid]["crane_start"],
                     "end_ts": "2026-09-30T00:30Z", "reason": "吊机故障"})
        # 受影响任务的两条占用全部释放并回待排；另一任务占用保持 held。
        for rid, status, reason in occ_states(self.hub, target_gid):
            self.assertEqual("released", status, rid)
            self.assertEqual("equipment_fault", reason)
        self.assertEqual("pending", task(self.hub, target_gid)["status"])
        other = [g for g in ("G1", "G2") if g != target_gid][0]
        for _rid, status, _reason in occ_states(self.hub, other):
            self.assertEqual("held", status)

        # 重排：新方案把受影响任务放到故障窗口之后，且不与任何 held 占用重叠。
        plan2 = plan_now(self.hub, "plan-2")["plan"]
        new_alloc = allocation_of(self.hub, plan2["plan_id"])
        self.assertIn(target_gid, new_alloc)
        confirm_all(self.hub, plan2["plan_id"])
        intervals = {}
        for e in self.hub.get_timeline("S1")["events"]:
            if e["kind"] == "occupancy" and e["status"] == "held":
                intervals.setdefault(e["resource_id"], []).append((e["start_ts"], e["end_ts"]))
        for resource, items in intervals.items():
            items.sort()
            for a, b in zip(items, items[1:]):
                self.assertLessEqual(a[1], b[0], f"{resource} 重排后仍冲突 {a} {b}")

    def test_delay_releases_occupancy_started_before_new_arrival(self):
        plan = plan_now(self.hub)["plan"]
        alloc = allocation_of(self.hub, plan["plan_id"])
        confirm_all(self.hub, plan["plan_id"])
        # G2 尚未进站交接：列车实际延误到 00:30，其早于新到达时刻的占用必须释放；
        # 已完成进站交接的 G1 责任在枢纽，占用不受列车消息影响。
        gid = "G2"
        delay_to = "2026-09-30T00:30Z"
        self.clock.advance(minutes=1)
        self.hub.ingest_event(
            request_id="delay", actor_id="railop", event_id="E-ARR-DELAY",
            event_type="arrival", occurred_at="2026-09-29T23:22Z",
            payload={"site_id": "S1", "batch_id": "TRAIN1", "actual_arrival": delay_to})
        reasons = {reason for _r, _s, reason in occ_states(self.hub, gid)}
        self.assertIn("arrival_delay", reasons)
        self.assertEqual("pending", task(self.hub, gid)["status"])
        for _rid, status, _reason in occ_states(self.hub, "G1"):
            self.assertEqual("held", status)

    def test_dropped_boxes_cancel_remaining_and_release_future(self):
        plan = plan_now(self.hub)["plan"]
        alloc = allocation_of(self.hub, plan["plan_id"])
        confirm_all(self.hub, plan["plan_id"])
        gid = "G2"
        self.clock.advance(minutes=1)
        # G2 整组 10 箱甩箱：任务取消，未来占用释放。
        self.hub.ingest_event(
            request_id="drop", actor_id="railop", event_id="E-DROP-G2",
            event_type="drop_qty", occurred_at="2026-09-29T23:22Z",
            payload={"site_id": "S1", "batch_id": "TRAIN1", "group_id": gid, "quantity": 10})
        self.assertEqual(0, task(self.hub, gid)["remaining_qty"])
        self.assertEqual("cancelled", task(self.hub, gid)["status"])
        for _rid, status, reason in occ_states(self.hub, gid):
            self.assertEqual("released", status)
            self.assertEqual("completed_or_dropped", reason)
        # G1 不受影响。
        for _rid, status, _reason in occ_states(self.hub, "G1"):
            self.assertEqual("held", status)

    def test_partial_completion_keeps_history_and_reschedules_rest(self):
        plan = plan_now(self.hub)["plan"]
        confirm_all(self.hub, plan["plan_id"])
        gid = "G1"
        # 先释放 G1（模拟其吊机短时故障），再完成 4 箱交接。
        alloc = allocation_of(self.hub, plan["plan_id"])
        self.clock.advance(minutes=1)
        self.hub.ingest_event(
            request_id="fault", actor_id="mgr", event_id="E-FAULT",
            event_type="equipment_fault", occurred_at="2026-09-29T23:22Z",
            payload={"site_id": "S1", "resource_id": alloc[gid]["crane"],
                     "start_ts": alloc[gid]["crane_start"],
                     "end_ts": "2026-09-30T00:30Z", "reason": "吊机故障"})
        self.hub.ingest_event(
            request_id="ho", actor_id="roadop", event_id="E-HO-G1",
            event_type="handover", occurred_at="2026-09-30T00:40Z",
            payload={"site_id": "S1", "shipment_id": "SHIP1", "group_id": gid,
                     "stage": "outbound", "quantity": 4, "at_ts": "2026-09-30T00:40Z"})
        t = task(self.hub, gid)
        self.assertEqual(4, t["completed_qty"])
        self.assertEqual(6, t["remaining_qty"])
        # 重排方案只为剩余 6 箱生成作业（占用数量与组绑定；排程只针对剩余任务）。
        plan2 = plan_now(self.hub, "plan-2")["plan"]
        self.assertIn(gid, {a["group_id"] for a in plan2["allocation"]})

    def test_historical_occupancy_is_never_released(self):
        plan = plan_now(self.hub)["plan"]
        alloc = allocation_of(self.hub, plan["plan_id"])
        confirm_all(self.hub, plan["plan_id"])
        # 把时钟推过全部占用结束时间后再登记封锁，历史占用必须保持 held 不动。
        self.clock.set(__import__("datetime").datetime(2026, 9, 30, 3, 0,
                      tzinfo=__import__("datetime").timezone.utc))
        gid = "G1"
        self.hub.register_blockade(
            request_id="late-blk", actor_id="mgr", blockade_id="BLK-LATE",
            resource_id=alloc[gid]["crane"],
            start_ts=alloc[gid]["crane_start"], end_ts=alloc[gid]["crane_end"],
            reason="事后封锁")
        for _rid, status, _reason in occ_states(self.hub, gid):
            self.assertEqual("held", status)


class FreezeKpiTest(unittest.TestCase):
    def setUp(self):
        self.database, self.svc, self.hub, self.clock = build_hub()
        two_group_world(self.hub)  # 两箱组均已进站交接

    def tearDown(self):
        self.database.close()

    def _complete(self, gid, qty, at_ts, req, eid):
        self.hub.ingest_event(
            request_id=req, actor_id="roadop", event_id=eid, event_type="handover",
            occurred_at=at_ts, payload={"site_id": "S1", "shipment_id": "SHIP1",
                                        "group_id": gid, "stage": "outbound",
                                        "quantity": qty, "at_ts": at_ts})

    def test_one_hour_rate_and_late_arrival_does_not_rewrite_frozen_report(self):
        window = ("2026-09-29T23:00Z", "2026-09-30T02:00Z")
        # 冻结时刻：G1 已在 30 分钟内出站 10 箱；G2 尚未出站交接。
        self._complete("G1", 10, "2026-09-29T23:55Z", "ho-g1", "E-HO-G1-OUT")
        report = self.hub.freeze_kpi(
            request_id="kpi", actor_id="mgr", site_id="S1",
            window_start=window[0], window_end=window[1])["report"]
        self.assertEqual(20, report["total_units"])
        self.assertEqual(10, report["on_time_units"])
        self.assertEqual(0.5, report["one_hour_rate"])
        report_id = report["report_id"]

        # 冻结后才补报 G2 实际在一小时内完成：实时状态改变，但冻结快照不被改写。
        self._complete("G2", 10, "2026-09-29T23:50Z", "ho-g2", "E-HO-G2-OUT")
        frozen = self.hub.get_kpi_report(report_id)
        self.assertEqual(10, frozen["on_time_units"])
        self.assertEqual(0.5, frozen["one_hour_rate"])
        # 重新冻结一份新报告才反映新事实。
        report2 = self.hub.freeze_kpi(
            request_id="kpi2", actor_id="mgr", site_id="S1",
            window_start=window[0], window_end=window[1])["report"]
        self.assertEqual(20, report2["on_time_units"])
        self.assertEqual(1.0, report2["one_hour_rate"])
        # 每票货可见责任与关键等待原因。
        reasons = {g["group_id"]: g["waiting_reason"]
                   for s in frozen["shipments"] for g in s["groups"]}
        self.assertEqual("completed", reasons["G1"])
        self.assertNotEqual("completed", reasons["G2"])

    def test_empty_window_rate_is_none(self):
        report = self.hub.freeze_kpi(
            request_id="kpi-empty", actor_id="mgr", site_id="S1",
            window_start="2026-09-30T03:00Z", window_end="2026-09-30T04:00Z")["report"]
        self.assertEqual(0, report["total_units"])
        self.assertIsNone(report["one_hour_rate"])


if __name__ == "__main__":
    unittest.main()
