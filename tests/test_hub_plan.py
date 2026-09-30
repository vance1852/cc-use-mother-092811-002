"""两阶段计划、承运方确认与原子占用的测试。"""

import unittest
from datetime import datetime, timezone

from transport_coordination.clock import MutableClock
from transport_coordination.errors import ConflictError, NotFoundError, PermissionDenied
from transport_coordination.storage import Database
from transport_coordination.hub import HubService
from transport_coordination.service import DomainService

from hub_support import build_hub


def seed_shipment(hub, ship="SHIP1", inbound="RAIL", outbound="ROAD",
                  groups=("G1", "G2"), contract=None):
    hub.register_shipment(
        request_id=f"ship-{ship}", actor_id="mgr", shipment_id=ship, site_id="S1",
        inbound_party=inbound, outbound_party=outbound, contract_id=contract,
        groups=[{"group_id": gid, "quantity": 10, "work_minutes": 20} for gid in groups])
    hub.register_batch(
        request_id=f"batch-in-{ship}", actor_id="mgr", batch_id=f"TRAIN-{ship}",
        site_id="S1", mode="rail", direction="inbound",
        planned_arrival="2026-09-29T23:30Z",
        manifest=[{"group_id": gid, "quantity": 10} for gid in groups])
    hub.register_batch(
        request_id=f"batch-out-{ship}", actor_id="mgr", batch_id=f"TRUCK-{ship}",
        site_id="S1", mode="road", direction="outbound",
        planned_arrival="2026-09-30T01:00Z", planned_departure="2026-09-30T01:30Z",
        manifest=[{"group_id": gid, "quantity": 10} for gid in groups])


def arrive_and_handover(hub, groups=("G1", "G2"), train="TRAIN-SHIP1", handover_groups=("G1", "G2")):
    hub.ingest_event(request_id="ev-arr", actor_id="railop", event_id=f"E-ARR-{train}",
                     event_type="arrival", occurred_at="2026-09-29T23:20Z",
                     payload={"site_id": "S1", "batch_id": train,
                              "actual_arrival": "2026-09-29T23:20Z"})
    for i, gid in enumerate(groups):
        hub.ingest_event(request_id=f"ev-aq-{gid}", actor_id="railop",
                         event_id=f"E-AQ-{train}-{gid}", event_type="arrival_qty",
                         occurred_at="2026-09-29T23:21Z",
                         payload={"site_id": "S1", "batch_id": train, "group_id": gid,
                                  "quantity": 10})
    for i, gid in enumerate(handover_groups):
        hub.ingest_event(request_id=f"ev-hi-{gid}", actor_id="railop",
                         event_id=f"E-HI-{gid}", event_type="handover",
                         occurred_at="2026-09-29T23:25Z",
                         payload={"site_id": "S1", "shipment_id": "SHIP1", "group_id": gid,
                                  "stage": "inbound", "quantity": 10,
                                  "at_ts": "2026-09-29T23:25Z"})


def held_intervals(hub, resource_id):
    events = hub.get_timeline("S1")["events"]
    return [(e["start_ts"], e["end_ts"]) for e in events
            if e["kind"] == "occupancy" and e["resource_id"] == resource_id
            and e["status"] == "held"]


class PlanTest(unittest.TestCase):
    def setUp(self):
        self.database, self.svc, self.hub, self.clock = build_hub()
        seed_shipment(self.hub)
        arrive_and_handover(self.hub)

    def tearDown(self):
        self.database.close()

    def _open_plan(self, req="plan-1"):
        return self.hub.create_plan(
            request_id=req, actor_id="mgr", site_id="S1",
            horizon_start="2026-09-29T23:00Z", horizon_end="2026-09-30T02:00Z")

    def test_plan_does_not_occupy_until_all_parties_confirm(self):
        plan = self._open_plan()
        self.assertEqual("open", plan["plan"]["status"])
        self.assertEqual(["RAIL", "ROAD"], plan["plan"]["required_parties"])
        self.assertEqual(0, len(held_intervals(self.hub, "CR1")))
        self.assertEqual(0, len(held_intervals(self.hub, "CR2")))

    def test_confirmations_are_per_party_and_atomic(self):
        plan = self._open_plan()
        plan_id = plan["plan"]["plan_id"]
        first = self.hub.confirm_plan(request_id="cf-r", actor_id="railop",
                                      plan_id=plan_id, party="RAIL")
        self.assertEqual("open", first["confirmation"]["status"])
        self.assertEqual(0, len(held_intervals(self.hub, "CR1")))
        with self.assertRaises(PermissionDenied):
            self.hub.confirm_plan(request_id="cf-sea", actor_id="seaop",
                                  plan_id=plan_id, party="RAIL")
        second = self.hub.confirm_plan(request_id="cf-d", actor_id="roadop",
                                       plan_id=plan_id, party="ROAD")
        self.assertEqual("confirmed", second["confirmation"]["status"])
        # 每个任务两条占用（吊机+堆位），共 4 条。
        self.assertEqual(4, len(second["confirmation"]["occupancies"]))
        self.assertEqual(2, len(held_intervals(self.hub, "CR1") + held_intervals(self.hub, "CR2")))

    def test_no_resource_is_double_promised(self):
        plan = self._open_plan()["plan"]["plan_id"]
        self.hub.confirm_plan(request_id="a", actor_id="railop", plan_id=plan, party="RAIL")
        self.hub.confirm_plan(request_id="b", actor_id="roadop", plan_id=plan, party="ROAD")
        # 新增第二票货箱组 G3，已到达待换装。
        seed_shipment(self.hub, ship="SHIP2", groups=("G3",))
        self.hub.ingest_event(request_id="arr2", actor_id="railop", event_id="E-ARR-TRAIN-SHIP2",
                              event_type="arrival", occurred_at="2026-09-29T23:40Z",
                              payload={"site_id": "S1", "batch_id": "TRAIN-SHIP2",
                                       "actual_arrival": "2026-09-29T23:40Z"})
        self.hub.ingest_event(request_id="aq3", actor_id="railop", event_id="E-AQ-TRAIN-SHIP2-G3",
                              event_type="arrival_qty", occurred_at="2026-09-29T23:41Z",
                              payload={"site_id": "S1", "batch_id": "TRAIN-SHIP2",
                                       "group_id": "G3", "quantity": 10})
        self.hub.ingest_event(request_id="hi3", actor_id="railop", event_id="E-HI-G3",
                              event_type="handover", occurred_at="2026-09-29T23:42Z",
                              payload={"site_id": "S1", "shipment_id": "SHIP2", "group_id": "G3",
                                       "stage": "inbound", "quantity": 10,
                                       "at_ts": "2026-09-29T23:42Z"})
        plan2 = self.hub.create_plan(
            request_id="plan-2", actor_id="mgr", site_id="S1",
            horizon_start="2026-09-29T23:00Z", horizon_end="2026-09-30T03:00Z")
        self.assertEqual(1, len(plan2["plan"]["allocation"]))
        plan2_id = plan2["plan"]["plan_id"]
        self.hub.confirm_plan(request_id="c", actor_id="railop", plan_id=plan2_id, party="RAIL")
        self.hub.confirm_plan(request_id="d", actor_id="roadop", plan_id=plan2_id, party="ROAD")
        # 逐资源校验：任意两条持有占用时间不重叠。
        for resource in ("CR1", "CR2", "SLOT1", "SLOT2"):
            intervals = sorted(held_intervals(self.hub, resource))
            for earlier, later in zip(intervals, intervals[1:]):
                self.assertLessEqual(earlier[1], later[0],
                                     f"{resource} 出现重复承诺 {earlier} {later}")

    def test_final_revalidation_rejects_whole_plan_with_zero_occupancies(self):
        plan = self._open_plan()["plan"]
        allocation = plan["allocation"]
        target = allocation[0]
        # 开口期间插入覆盖该任务吊机窗口的封锁：最终确认必须整单拒绝。
        self.hub.register_blockade(
            request_id="blk-1", actor_id="mgr", blockade_id="BLK1",
            resource_id=target["crane"], start_ts=target["crane_start"],
            end_ts=target["crane_end"], reason="临时封锁")
        plan_id = plan["plan_id"]
        self.hub.confirm_plan(request_id="x", actor_id="railop", plan_id=plan_id, party="RAIL")
        result = self.hub.confirm_plan(request_id="y", actor_id="roadop",
                                       plan_id=plan_id, party="ROAD")
        self.assertEqual("rejected", result["confirmation"]["status"])
        self.assertTrue(result["confirmation"]["conflicts"])
        # 零占用写入：所有资源均无 held 记录。
        for resource in ("CR1", "CR2", "SLOT1", "SLOT2"):
            self.assertEqual(0, len(held_intervals(self.hub, resource)))
        for task in self.hub.list_tasks("S1")["tasks"]:
            self.assertNotEqual("planned", task["status"])
            self.assertIsNone(task["plan_id"])

    def test_plan_expires_after_ttl(self):
        plan = self.hub.create_plan(
            request_id="plan-ttl", actor_id="mgr", site_id="S1",
            horizon_start="2026-09-29T23:00Z", horizon_end="2026-09-30T02:00Z",
            ttl_minutes=10)["plan"]
        self.clock.advance(minutes=11)
        result = self.hub.confirm_plan(request_id="late", actor_id="railop",
                                       plan_id=plan["plan_id"], party="RAIL")
        self.assertEqual("expired", result["confirmation"]["status"])
        with self.assertRaises(ConflictError):
            self.hub.confirm_plan(request_id="late2", actor_id="roadop",
                                  plan_id=plan["plan_id"], party="ROAD")

    def test_new_plan_supersedes_open_plan(self):
        first = self._open_plan("plan-a")["plan"]
        second = self._open_plan("plan-b")["plan"]
        self.assertEqual("superseded", self._plan_status(first["plan_id"]))
        with self.assertRaises(ConflictError):
            self.hub.confirm_plan(request_id="old", actor_id="railop",
                                  plan_id=first["plan_id"], party="RAIL")
        self.assertEqual("open", second["status"])

    def test_duplicate_confirmation_is_rejected(self):
        plan_id = self._open_plan()["plan"]["plan_id"]
        self.hub.confirm_plan(request_id="one", actor_id="railop", plan_id=plan_id, party="RAIL")
        with self.assertRaises(ConflictError):
            self.hub.confirm_plan(request_id="two", actor_id="railop", plan_id=plan_id, party="RAIL")

    def _plan_status(self, plan_id):
        row = self.database.connection.execute(
            "SELECT status FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
        return row["status"]


if __name__ == "__main__":
    unittest.main()
