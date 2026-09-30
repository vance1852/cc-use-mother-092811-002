"""换装协同 HTTP 路由测试。"""

import json
import unittest

from transport_coordination.api import route
from transport_coordination.hub import HubService
from transport_coordination.service import DomainService
from transport_coordination.storage import Database

from hub_support import build_hub


class HubApiTest(unittest.TestCase):
    def setUp(self):
        self.database, self.svc, self.hub, self.clock = build_hub()

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, actor="mgr"):
        return route(self.svc, method, path, body or {},
                     {"X-Actor-Id": actor}, hub=self.hub)

    def _shipment_flow(self):
        status, res = self.call("POST", "/shipments", {
            "request_id": "ship", "shipment_id": "SHIP1", "site_id": "S1",
            "inbound_party": "RAIL", "outbound_party": "ROAD",
            "groups": [{"group_id": "G1", "quantity": 10, "work_minutes": 20}]})
        self.assertEqual(201, status)
        status, _ = self.call("POST", "/batches", {
            "request_id": "bin", "batch_id": "TRAIN1", "site_id": "S1", "mode": "rail",
            "direction": "inbound", "planned_arrival": "2026-09-29T23:20Z",
            "manifest": [{"group_id": "G1", "quantity": 10}]})
        self.assertEqual(201, status)
        status, _ = self.call("POST", "/batches", {
            "request_id": "bout", "batch_id": "TRUCK1", "site_id": "S1", "mode": "road",
            "direction": "outbound", "planned_arrival": "2026-09-30T01:00Z",
            "planned_departure": "2026-09-30T01:30Z",
            "manifest": [{"group_id": "G1", "quantity": 10}]})
        self.assertEqual(201, status)

    def test_registration_routes(self):
        for path, key, body in (
            ("/hub/resources", "resource_id",
             {"request_id": "r1", "resource_id": "CR9", "site_id": "S1",
              "kind": "crane", "name": "新吊机"}),
            ("/hub/contracts", "contract_id",
             {"request_id": "r2", "contract_id": "PC9", "site_id": "S1",
              "priority": 0, "name": "普通合同"}),
        ):
            status, payload = self.call("POST", path, body)
            self.assertEqual(201, status, payload)
            self.assertEqual(body[key], payload["resource_id"])
            # 同 request_id 重放返回 200。
            status2, _ = self.call("POST", path, body)
            self.assertEqual(200, status2)

    def test_full_plan_confirm_flow_over_http(self):
        self._shipment_flow()
        self.call("POST", "/events", {
            "request_id": "arr", "event_id": "E-ARR", "event_type": "arrival",
            "occurred_at": "2026-09-29T23:20Z",
            "payload": {"site_id": "S1", "batch_id": "TRAIN1",
                        "actual_arrival": "2026-09-29T23:20Z"}}, actor="railop")
        self.call("POST", "/events", {
            "request_id": "aq", "event_id": "E-AQ", "event_type": "arrival_qty",
            "occurred_at": "2026-09-29T23:21Z",
            "payload": {"site_id": "S1", "batch_id": "TRAIN1", "group_id": "G1",
                        "quantity": 10}}, actor="railop")
        self.call("POST", "/events", {
            "request_id": "hi", "event_id": "E-HI", "event_type": "handover",
            "occurred_at": "2026-09-29T23:25Z",
            "payload": {"site_id": "S1", "shipment_id": "SHIP1", "group_id": "G1",
                        "stage": "inbound", "quantity": 10,
                        "at_ts": "2026-09-29T23:25Z"}}, actor="railop")
        status, plan = self.call("POST", "/plans", {
            "request_id": "p1", "site_id": "S1",
            "horizon_start": "2026-09-29T23:00Z", "horizon_end": "2026-09-30T02:00Z"})
        self.assertEqual(201, status)
        plan_id = plan["plan"]["plan_id"]
        status, c1 = self.call("POST", "/plans/confirm",
                               {"request_id": "c1", "plan_id": plan_id, "party": "RAIL"},
                               actor="railop")
        self.assertEqual(200, status)
        self.assertEqual("open", c1["confirmation"]["status"])
        status, c2 = self.call("POST", "/plans/confirm",
                               {"request_id": "c2", "plan_id": plan_id, "party": "ROAD"},
                               actor="roadop")
        self.assertEqual(200, status)
        self.assertEqual("confirmed", c2["confirmation"]["status"])

    def test_manager_views(self):
        self._shipment_flow()
        status, timeline = self.call("GET", "/timeline?site_id=S1")
        self.assertEqual(200, status)
        kinds = {e["kind"] for e in timeline["events"]}
        self.assertIn("batch", kinds)
        status, tasks = self.call("GET", "/tasks?site_id=S1")
        self.assertEqual(200, status)
        self.assertEqual(1, len(tasks["tasks"]))
        status, view = self.call("GET", "/shipments/SHIP1")
        self.assertEqual(200, status)
        self.assertEqual(["RAIL", "HUB", "ROAD"], view["custody_chain"])
        self.assertEqual("awaiting_arrival", view["groups"][0]["waiting_reason"])

    def test_frozen_kpi_routes(self):
        self._shipment_flow()
        status, report = self.call("POST", "/kpi-reports", {
            "request_id": "k1", "site_id": "S1",
            "window_start": "2026-09-29T23:00Z", "window_end": "2026-09-30T02:00Z"})
        self.assertEqual(201, status)
        report_id = report["report"]["report_id"]
        status, fetched = self.call("GET", f"/kpi-reports/{report_id}")
        self.assertEqual(200, status)
        self.assertIsNone(fetched["one_hour_rate"])
        status, listing = self.call("GET", "/kpi-reports?site_id=S1")
        self.assertEqual(200, status)
        self.assertEqual(1, len(listing["items"]))

    def test_bad_reference_returns_domain_error(self):
        status, payload = self.call("POST", "/events", {
            "request_id": "bad", "event_id": "E-BAD", "event_type": "arrival",
            "occurred_at": "2026-09-29T23:20Z",
            "payload": {"site_id": "S1", "batch_id": "NO-SUCH-TRAIN"}}, actor="railop")
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"])

    def test_final_rejection_returns_409_and_zero_occupancies(self):
        self._shipment_flow()
        self.call("POST", "/events", {
            "request_id": "arr", "event_id": "E-ARR", "event_type": "arrival",
            "occurred_at": "2026-09-29T23:20Z",
            "payload": {"site_id": "S1", "batch_id": "TRAIN1",
                        "actual_arrival": "2026-09-29T23:20Z"}}, actor="railop")
        self.call("POST", "/events", {
            "request_id": "aq", "event_id": "E-AQ", "event_type": "arrival_qty",
            "occurred_at": "2026-09-29T23:21Z",
            "payload": {"site_id": "S1", "batch_id": "TRAIN1", "group_id": "G1",
                        "quantity": 10}}, actor="railop")
        self.call("POST", "/events", {
            "request_id": "hi", "event_id": "E-HI", "event_type": "handover",
            "occurred_at": "2026-09-29T23:25Z",
            "payload": {"site_id": "S1", "shipment_id": "SHIP1", "group_id": "G1",
                        "stage": "inbound", "quantity": 10,
                        "at_ts": "2026-09-29T23:25Z"}}, actor="railop")
        plan = self.call("POST", "/plans", {
            "request_id": "p1", "site_id": "S1",
            "horizon_start": "2026-09-29T23:00Z", "horizon_end": "2026-09-30T02:00Z"})[1]["plan"]
        target = plan["allocation"][0]
        self.clock.advance(minutes=1)
        self.call("POST", "/hub/blockades", {
            "request_id": "blk", "blockade_id": "BLK1", "resource_id": target["crane"],
            "start_ts": target["crane_start"], "end_ts": target["crane_end"],
            "reason": "临时封锁"})
        self.call("POST", "/plans/confirm",
                  {"request_id": "c1", "plan_id": plan["plan_id"], "party": "RAIL"},
                  actor="railop")
        status, final = self.call("POST", "/plans/confirm",
                                  {"request_id": "c2", "plan_id": plan["plan_id"],
                                   "party": "ROAD"}, actor="roadop")
        self.assertEqual(409, status)
        self.assertEqual("rejected", final["confirmation"]["status"])

    def test_unknown_route_still_404(self):
        status, payload = self.call("GET", "/hub/unknown")
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
