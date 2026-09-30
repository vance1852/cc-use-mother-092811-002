import unittest
from datetime import datetime, timezone

from transport_coordination.clock import ManualClock
from transport_coordination.errors import ConflictError, PermissionDenied, ValidationError
from transport_coordination.hub_service import HubService
from transport_coordination.service import DomainService
from transport_coordination.storage import Database

SITE = "hub-site"
T0 = datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)


class HubTestCase(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = ManualClock(T0)
        self.domain = DomainService(self.database, self.clock)
        self.hub = HubService(self.database, self.clock)
        self._bootstrap()

    def tearDown(self):
        self.database.close()

    def _bootstrap(self):
        d, h = self.domain, self.hub
        d.register_organization(request_id="org-hub", actor_id="bootstrap",
                                organization_id="hub-001", name="枢纽运营中心")
        d.register_actor(request_id="actor-admin", actor_id="bootstrap",
                         new_actor_id="admin-hub", display_name="枢纽管理员",
                         role="admin", organization_id="hub-001")
        for req, org, actor, name in (
                ("org-rail", "rail-001", "op-rail", "铁路"),
                ("org-port", "port-001", "op-port", "港区"),
                ("org-truck", "truck-001", "op-truck", "车队")):
            d.register_organization(request_id=req, actor_id="admin-hub",
                                    organization_id=org, name=name)
            d.register_actor(request_id=req + "-actor", actor_id="admin-hub",
                             new_actor_id=actor, display_name=name,
                             role="operator", organization_id=org)
        d.register_site(request_id="site", actor_id="admin-hub", site_id=SITE,
                        organization_id="hub-001", name="枢纽", timezone_name="Asia/Shanghai")
        for req, rid, kind, name in (
                ("res-c1", "crane-1", "crane", "吊机一"),
                ("res-c2", "crane-2", "crane", "吊机二"),
                ("res-s1", "slot-1", "slot", "堆位一"),
                ("res-s2", "slot-2", "slot", "堆位二")):
            h.register_resource(request_id=req, actor_id="admin-hub", site_id=SITE,
                                resource_id=rid, kind=kind, name=name)

    def _register_basic_shipments(self):
        h = self.hub
        h.register_contract(request_id="contract-p1", actor_id="admin-hub", site_id=SITE,
                            contract_id="p1", title="优先合同", priority_rank=0)
        h.register_batch(request_id="batch-rail", actor_id="admin-hub", site_id=SITE,
                         batch_id="rail-1", mode="rail", planned_arrival="2026-09-26T01:00:00Z")
        h.register_batch(request_id="batch-vessel", actor_id="admin-hub", site_id=SITE,
                         batch_id="vessel-1", mode="vessel",
                         planned_arrival="2026-09-26T02:00:00Z")
        h.register_shipment(
            request_id="ship-1", actor_id="admin-hub", site_id=SITE, shipment_id="sh-1",
            batch_id="rail-1", contract_id="p1", inbound_party="rail-001",
            outbound_party="port-001", duration_minutes=30,
            containers=[{"container_id": "box-1", "group_id": "g1", "position": 0},
                        {"container_id": "box-2", "group_id": "g1", "position": 1}])
        h.register_shipment(
            request_id="ship-2", actor_id="admin-hub", site_id=SITE, shipment_id="sh-2",
            batch_id="vessel-1", inbound_party="port-001", outbound_party="truck-001",
            duration_minutes=30,
            containers=[{"container_id": "box-3", "group_id": "g1", "position": 0}])

    def _propose_confirm_commit(self, req="plan", valid=600):
        plan = self.hub.create_plan(request_id=req, actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=valid)
        plan_id = plan["plan_id"]
        for actor in ("op-rail", "op-port", "op-truck"):
            self.hub.confirm_plan(request_id=f"{req}-cf-{actor}", actor_id=actor,
                                  plan_id=plan_id)
        self.hub.commit_plan(request_id=f"{req}-cm", actor_id="admin-hub", plan_id=plan_id)
        return plan_id

    def _arrive(self, batch_id, arrival_at, event_id, req, at="2026-09-25T20:00:00Z",
                sequence=1, etype="arrival.early", actor="op-rail"):
        return self.hub.ingest_event(
            request_id=req, actor_id=actor, site_id=SITE,
            event={"event_id": event_id, "event_type": etype, "occurred_at": at,
                   "sequence": sequence, "batch_id": batch_id, "arrival_at": arrival_at})


class PlanLifecycleTest(HubTestCase):
    def test_plan_never_double_books_crane_or_slot(self):
        self._register_basic_shipments()
        plan = self.hub.create_plan(request_id="plan-a", actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=600)
        windows = [(t["crane_id"], t["slot_id"], t["start_ts"], t["end_ts"])
                   for t in plan["tasks"]]

        def overlap(a_start, a_end, b_start, b_end) -> bool:
            return a_start < b_end and b_start < a_end

        for i, (crane_a, slot_a, start_a, end_a) in enumerate(windows):
            for crane_b, slot_b, start_b, end_b in windows[i + 1:]:
                if crane_a == crane_b:
                    self.assertFalse(overlap(start_a, end_a, start_b, end_b))
                if slot_a == slot_b:
                    self.assertFalse(overlap(start_a, end_a, start_b, end_b))

    def test_proposal_expires_and_cannot_be_confirmed(self):
        self._register_basic_shipments()
        plan = self.hub.create_plan(request_id="plan-exp", actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=30)
        self.clock.advance(minutes=31)
        with self.assertRaises(ConflictError):
            self.hub.confirm_plan(request_id="cf-late", actor_id="op-rail",
                                  plan_id=plan["plan_id"])
        self.assertEqual("lapsed", self.hub.get_plan(plan["plan_id"])["state"])

    def test_commit_requires_all_carrier_confirmations(self):
        self._register_basic_shipments()
        plan = self.hub.create_plan(request_id="plan-mid", actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=600)
        self.hub.confirm_plan(request_id="cf-rail", actor_id="op-rail",
                              plan_id=plan["plan_id"])
        with self.assertRaises(ConflictError):
            self.hub.commit_plan(request_id="cm-mid", actor_id="admin-hub",
                                 plan_id=plan["plan_id"])

    def test_unrelated_carrier_cannot_confirm(self):
        self._register_basic_shipments()
        plan = self.hub.create_plan(request_id="plan-iso", actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=600)
        with self.assertRaises(PermissionDenied):
            self.hub.confirm_plan(request_id="cf-iso", actor_id="admin-hub",
                                  plan_id=plan["plan_id"])

    def test_commit_is_atomic_and_blocks_later_plans(self):
        self._register_basic_shipments()
        plan_id = self._propose_confirm_commit("plan-1")
        view = self.hub.get_plan(plan_id)
        self.assertEqual("committed", view["state"])
        holds = self.database.connection.execute(
            "SELECT COUNT(*) AS c FROM hub_holds").fetchone()["c"]
        self.assertEqual(2 * len(view["tasks"]), holds)
        # 重复提交同一方案直接返回 committed，不重复落锁
        again = self.hub.commit_plan(request_id="cm-retry", actor_id="admin-hub",
                                     plan_id=plan_id)
        self.assertTrue(again["replayed"])
        self.assertEqual(holds, self.database.connection.execute(
            "SELECT COUNT(*) AS c FROM hub_holds").fetchone()["c"])

    def test_blockade_window_moves_schedule(self):
        self._register_basic_shipments()
        self.hub.register_blockade(
            request_id="block", actor_id="admin-hub", site_id=SITE, resource_id="crane-1",
            start_ts="2026-09-26T01:00:00Z", end_ts="2026-09-26T01:40:00Z",
            reason="夜间检修")
        plan = self.hub.create_plan(request_id="plan-blk", actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=600)
        sh1 = next(t for t in plan["tasks"] if t["shipment_id"] == "sh-1")
        self.assertEqual("crane-2", sh1["crane_id"])


class RescheduleTest(HubTestCase):
    def test_early_arrival_only_reschedules_affected_batch(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        result = self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-req")
        shipments = {t["shipment_id"] for t in
                     self.hub.get_plan(result["rescheduled_plan_id"])["tasks"]}
        self.assertEqual({"sh-1"}, shipments)
        self.assertEqual(["sh-1"], [r["shipment_id"] for r in result["released_tasks"]])

    def test_delay_then_stale_confirmation_does_not_change_timeline(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        self._arrive("vessel-1", "2026-09-26T02:20:00Z", "ev-delay", "ev-delay-req",
                     at="2026-09-25T22:00:00Z", etype="arrival.delayed", actor="op-port")
        rate_delayed = self.hub.compute_rate(SITE, "2026-09-26T03:30:00Z")
        # 乱序补发更早的确认消息：不重排，口径不变
        stale = self._arrive("vessel-1", "2026-09-26T02:00:00Z", "ev-confirm",
                             "ev-confirm-req", at="2026-09-25T21:00:00Z",
                             etype="arrival.confirmed", actor="op-port")
        self.assertEqual([], stale["released_tasks"])
        self.assertEqual(rate_delayed, self.hub.compute_rate(SITE, "2026-09-26T03:30:00Z"))

    def test_skipped_container_shrinks_group_and_conserves_quantity(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-early-req")
        result = self.hub.ingest_event(
            request_id="ev-skip-req", actor_id="op-rail", site_id=SITE,
            event={"event_id": "ev-skip", "event_type": "arrival.skipped",
                   "occurred_at": "2026-09-26T00:31:00Z", "sequence": 2,
                   "batch_id": "rail-1", "container_ids": ["box-2"], "reason": "破损"})
        task = self.hub.get_plan(result["rescheduled_plan_id"])["tasks"][0]
        self.assertEqual(["box-1"], task["container_ids"])
        report = self.hub.conservation(SITE)["shipments"]["sh-1"]
        self.assertEqual(2, report["registered"])
        self.assertEqual(1, report["skipped"])
        self.assertEqual(1, report["arrived_pending"])

    def test_equipment_failure_releases_only_overlapping_unfinished_tasks(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-early-req")
        # sh-1 先完成交接
        self.hub.ingest_event(
            request_id="ev-work", actor_id="op-port", site_id=SITE,
            event={"event_id": "ev-work", "event_type": "work.partial",
                   "occurred_at": "2026-09-26T00:55:00Z", "sequence": 9,
                   "shipment_id": "sh-1", "container_ids": ["box-1", "box-2"]})
        failure = self.hub.ingest_event(
            request_id="ev-fail-req", actor_id="admin-hub", site_id=SITE,
            event={"event_id": "ev-fail", "event_type": "equipment.failed",
                   "occurred_at": "2026-09-26T00:40:00Z", "sequence": 3,
                   "resource_id": "crane-1", "end_ts": "2026-09-26T01:20:00Z"})
        released = {r["shipment_id"] for r in failure["released_tasks"]}
        # sh-1 虽与故障窗口重叠，但在当前最新方案中的任务已交接，不再重排
        self.assertNotIn("sh-1", released)

    def test_completed_handover_never_rolls_back(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-early-req")
        self.hub.ingest_event(
            request_id="ev-work", actor_id="op-port", site_id=SITE,
            event={"event_id": "ev-work", "event_type": "work.partial",
                   "occurred_at": "2026-09-26T00:55:00Z", "sequence": 4,
                   "shipment_id": "sh-1", "container_ids": ["box-1", "box-2"]})
        # 已交接后再甩箱必须被拒绝
        with self.assertRaises(ValidationError):
            self.hub.ingest_event(
                request_id="ev-skip-late", actor_id="op-rail", site_id=SITE,
                event={"event_id": "ev-skip-late", "event_type": "arrival.skipped",
                       "occurred_at": "2026-09-26T01:10:00Z", "sequence": 5,
                       "batch_id": "rail-1", "container_ids": ["box-1"]})
        view = self.hub.get_shipment(SITE, "sh-1")
        self.assertEqual("completed", view["handover_stage"])
        self.assertEqual("port-001", view["custodian"])

    def test_handover_before_arrival_is_rejected(self):
        self._register_basic_shipments()
        with self.assertRaises(ValidationError):
            self.hub.ingest_event(
                request_id="ev-impossible", actor_id="op-port", site_id=SITE,
                event={"event_id": "ev-impossible", "event_type": "work.partial",
                       "occurred_at": "2026-09-26T00:50:00Z", "sequence": 1,
                       "shipment_id": "sh-1", "container_ids": ["box-1"]})


class ReplayAndOrderTest(HubTestCase):
    def test_same_event_replays_original_result(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        first = self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-x", "req-first")
        # 相同 event_id、相同内容、不同 request_id：返回原结果
        second = self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-x", "req-second")
        self.assertTrue(second["replayed"])
        self.assertEqual(first["rescheduled_plan_id"], second["rescheduled_plan_id"])
        self.assertEqual(first["released_tasks"], second["released_tasks"])
        # 相同 event_id、不同内容：冲突
        with self.assertRaises(ConflictError):
            self._arrive("rail-1", "2026-09-26T00:20:00Z", "ev-x", "req-third")

    def test_different_delivery_order_produces_same_rate(self):
        def build(ordered):
            database = Database()
            clock = ManualClock(T0)
            domain = DomainService(database, clock)
            hub = HubService(database, clock)
            domain.register_organization(request_id="org", actor_id="bootstrap",
                                         organization_id="hub-001", name="枢纽")
            domain.register_actor(request_id="adm", actor_id="bootstrap",
                                  new_actor_id="admin-hub", display_name="管理员",
                                  role="admin", organization_id="hub-001")
            domain.register_site(request_id="sit", actor_id="admin-hub", site_id=SITE,
                                 organization_id="hub-001", name="枢纽",
                                 timezone_name="Asia/Shanghai")
            hub.register_resource(request_id="rc", actor_id="admin-hub", site_id=SITE,
                                  resource_id="crane-1", kind="crane", name="吊机")
            hub.register_resource(request_id="rs", actor_id="admin-hub", site_id=SITE,
                                  resource_id="slot-1", kind="slot", name="堆位")
            hub.register_batch(request_id="br", actor_id="admin-hub", site_id=SITE,
                               batch_id="rail-1", mode="rail",
                               planned_arrival="2026-09-26T01:00:00Z")
            hub.register_shipment(
                request_id="sh", actor_id="admin-hub", site_id=SITE, shipment_id="sh-1",
                batch_id="rail-1", inbound_party="party-a", outbound_party="party-b",
                duration_minutes=20,
                containers=[{"container_id": "box-1", "group_id": "g1", "position": 0}])
            for index, event in enumerate(ordered):
                hub.ingest_event(request_id=f"e{index}", actor_id="admin-hub",
                                 site_id=SITE, event=event)
            rate = hub.compute_rate(SITE, "2026-09-26T03:00:00Z")
            database.close()
            return rate

        early = {"event_id": "ev-early", "event_type": "arrival.early",
                 "occurred_at": "2026-09-25T20:00:00Z", "sequence": 1,
                 "batch_id": "rail-1", "arrival_at": "2026-09-26T00:30:00Z"}
        confirm = {"event_id": "ev-plan", "event_type": "arrival.confirmed",
                   "occurred_at": "2026-09-25T18:00:00Z", "sequence": 0,
                   "batch_id": "rail-1", "arrival_at": "2026-09-26T01:00:00Z"}
        work = {"event_id": "ev-work", "event_type": "work.partial",
                "occurred_at": "2026-09-26T00:50:00Z", "sequence": 2,
                "shipment_id": "sh-1", "container_ids": ["box-1"]}
        rate_a = build([confirm, early, work])
        rate_b = build([early, confirm, work])
        self.assertEqual(rate_a, rate_b)
        self.assertEqual(1, rate_a["arrived_count"])
        self.assertEqual(1.0, rate_a["rate"])


class RateAndWaitingTest(HubTestCase):
    def test_freeze_is_immutable_and_skipped_boxes_excluded(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-early-req")
        self.hub.ingest_event(
            request_id="ev-skip", actor_id="op-rail", site_id=SITE,
            event={"event_id": "ev-skip", "event_type": "arrival.skipped",
                   "occurred_at": "2026-09-26T00:31:00Z", "sequence": 2,
                   "batch_id": "rail-1", "container_ids": ["box-2"]})
        self.hub.ingest_event(
            request_id="ev-work", actor_id="op-port", site_id=SITE,
            event={"event_id": "ev-work", "event_type": "work.partial",
                   "occurred_at": "2026-09-26T00:50:00Z", "sequence": 3,
                   "shipment_id": "sh-1", "container_ids": ["box-1"]})
        freeze = self.hub.freeze_rate(request_id="freeze-1", actor_id="admin-hub",
                                      site_id=SITE, as_of="2026-09-26T01:30:00Z")
        self.assertEqual(1, freeze["arrived_count"])
        self.assertEqual(1.0, freeze["rate"])
        with self.assertRaises(ConflictError):
            self.hub.freeze_rate(request_id="freeze-2", actor_id="admin-hub",
                                 site_id=SITE, as_of="2026-09-26T01:30:00Z")
        stored = self.hub.list_freezes(SITE)
        self.assertEqual(1, len(stored))

    def test_waiting_reason_before_arrival(self):
        self._register_basic_shipments()
        self.clock.set(datetime(2026, 9, 26, 0, 20, tzinfo=timezone.utc))
        self.assertEqual("waiting_arrival",
                         self.hub.get_shipment(SITE, "sh-1")["waiting_reason"])

    def test_waiting_reason_equipment_failure(self):
        self._register_basic_shipments()
        self._propose_confirm_commit("plan-base")
        self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-early-req")
        self.clock.set(datetime(2026, 9, 26, 0, 45, tzinfo=timezone.utc))
        self.hub.ingest_event(
            request_id="ev-fail", actor_id="admin-hub", site_id=SITE,
            event={"event_id": "ev-fail", "event_type": "equipment.failed",
                   "occurred_at": "2026-09-26T00:40:00Z", "sequence": 3,
                   "resource_id": "crane-1", "end_ts": "2026-09-26T01:20:00Z"})
        self.hub.ingest_event(
            request_id="ev-fail2", actor_id="admin-hub", site_id=SITE,
            event={"event_id": "ev-fail2", "event_type": "equipment.failed",
                   "occurred_at": "2026-09-26T00:40:00Z", "sequence": 3,
                   "resource_id": "crane-2", "end_ts": "2026-09-26T01:20:00Z"})
        reason = self.hub.get_shipment(SITE, "sh-1")["waiting_reason"]
        self.assertEqual("equipment_failed", reason)

    def test_waiting_reason_blockade_window(self):
        self._register_basic_shipments()
        # 两台吊机在同一时段检修，全部吊机不可用
        for req, crane in (("bk1", "crane-1"), ("bk2", "crane-2")):
            self.hub.register_blockade(
                request_id=req, actor_id="admin-hub", site_id=SITE, resource_id=crane,
                start_ts="2026-09-26T00:40:00Z", end_ts="2026-09-26T01:20:00Z",
                reason="联合检修")
        self._arrive("rail-1", "2026-09-26T00:30:00Z", "ev-early", "ev-early-req")
        self.clock.set(datetime(2026, 9, 26, 0, 45, tzinfo=timezone.utc))
        self.assertEqual("blockade_window",
                         self.hub.get_shipment(SITE, "sh-1")["waiting_reason"])

    def test_waiting_reason_resource_contention(self):
        self._register_basic_shipments()
        self.hub.register_blockade(
            request_id="blk1", actor_id="admin-hub", site_id=SITE, resource_id="crane-1",
            start_ts="2026-09-26T01:00:00Z", end_ts="2026-09-26T01:40:00Z", reason="检修")
        # 另一列车在 01:00 同时到货，两台吊机一台检修、一台被 sh-1 占用
        self.hub.register_batch(request_id="batch-rail2", actor_id="admin-hub", site_id=SITE,
                                batch_id="rail-2", mode="rail",
                                planned_arrival="2026-09-26T01:00:00Z")
        self.hub.register_shipment(
            request_id="ship-3", actor_id="admin-hub", site_id=SITE, shipment_id="sh-3",
            batch_id="rail-2", inbound_party="rail-001", outbound_party="truck-001",
            duration_minutes=30,
            containers=[{"container_id": "box-4", "group_id": "g1", "position": 0}])
        self._propose_confirm_commit("plan-base")
        self.clock.set(datetime(2026, 9, 26, 1, 0, tzinfo=timezone.utc))
        self.assertEqual("resource_contention",
                         self.hub.get_shipment(SITE, "sh-3")["waiting_reason"])

    def test_priority_contract_scheduled_first(self):
        self._register_basic_shipments()
        # 再来一票无优先合同、同时刻就绪的货物
        self.hub.register_batch(request_id="batch-truck", actor_id="admin-hub", site_id=SITE,
                                batch_id="truck-1", mode="vehicle",
                                planned_arrival="2026-09-26T01:00:00Z")
        self.hub.register_shipment(
            request_id="ship-9", actor_id="admin-hub", site_id=SITE, shipment_id="sh-9",
            batch_id="truck-1", inbound_party="truck-001", outbound_party="port-001",
            duration_minutes=30,
            containers=[{"container_id": "box-9", "group_id": "g1", "position": 0}])
        plan = self.hub.create_plan(request_id="plan-prio", actor_id="admin-hub",
                                    site_id=SITE, valid_minutes=600)
        first_shipment = plan["tasks"][0]["shipment_id"]
        self.assertEqual("sh-1", first_shipment)


if __name__ == "__main__":
    unittest.main()
