import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from transport_coordination.api import route
from transport_coordination.clock import ManualClock
from transport_coordination.hub_service import HubService
from transport_coordination.service import DomainService
from transport_coordination.storage import Database

T0 = datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)


def _build(database, clock):
    domain = DomainService(database, clock)
    hub = HubService(database, clock)
    domain.register_organization(request_id="org", actor_id="bootstrap",
                                 organization_id="hub-001", name="枢纽")
    domain.register_actor(request_id="adm", actor_id="bootstrap", new_actor_id="admin-hub",
                          display_name="管理员", role="admin", organization_id="hub-001")
    domain.register_actor(request_id="admr", actor_id="admin-hub", new_actor_id="op-rail",
                          display_name="铁路员", role="operator", organization_id="hub-001")
    domain.register_site(request_id="sit", actor_id="admin-hub", site_id="s1",
                         organization_id="hub-001", name="枢纽", timezone_name="Asia/Shanghai")
    domain.register_organization(request_id="org-2", actor_id="admin-hub",
                                 organization_id="org-002", name="接收方")
    domain.register_actor(request_id="adm-2", actor_id="admin-hub", new_actor_id="op-b",
                          display_name="接收方值班员", role="operator",
                          organization_id="org-002")
    hub.register_resource(request_id="rc", actor_id="admin-hub", site_id="s1",
                          resource_id="crane-1", kind="crane", name="吊机")
    hub.register_resource(request_id="rs", actor_id="admin-hub", site_id="s1",
                          resource_id="slot-1", kind="slot", name="堆位")
    hub.register_batch(request_id="br", actor_id="admin-hub", site_id="s1", batch_id="b1",
                       mode="rail", planned_arrival="2026-09-26T01:00:00Z")
    hub.register_shipment(
        request_id="sh", actor_id="admin-hub", site_id="s1", shipment_id="sh-1",
        batch_id="b1", inbound_party="hub-001", outbound_party="org-002",
        duration_minutes=20,
        containers=[{"container_id": "box-1", "group_id": "g1", "position": 0}])
    return domain, hub


class PersistenceRecoveryTest(unittest.TestCase):
    def test_state_and_rate_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hub.sqlite3"
            clock = ManualClock(T0)
            db = Database(path)
            _, hub = _build(db, clock)
            hub.ingest_event(
                request_id="ev", actor_id="admin-hub", site_id="s1",
                event={"event_id": "ev-early", "event_type": "arrival.early",
                       "occurred_at": "2026-09-25T20:00:00Z", "sequence": 1,
                       "batch_id": "b1", "arrival_at": "2026-09-26T00:30:00Z"})
            hub.ingest_event(
                request_id="wk", actor_id="admin-hub", site_id="s1",
                event={"event_id": "ev-work", "event_type": "work.partial",
                       "occurred_at": "2026-09-26T00:50:00Z", "sequence": 2,
                       "shipment_id": "sh-1", "container_ids": ["box-1"]})
            rate_before = hub.compute_rate("s1", "2026-09-26T02:00:00Z")
            view_before = hub.get_shipment("s1", "sh-1")
            valid_before, count_before = DomainService(db, clock).verify_audit()
            db.close()

            db2 = Database(path)
            hub2 = HubService(db2, ManualClock(T0))
            rate_after = hub2.compute_rate("s1", "2026-09-26T02:00:00Z")
            view_after = hub2.get_shipment("s1", "sh-1")
            valid_after, count_after = DomainService(db2, ManualClock(T0)).verify_audit()
            db2.close()

            self.assertEqual(rate_before, rate_after)
            self.assertEqual(view_before, view_after)
            self.assertTrue(valid_before and valid_after)
            self.assertEqual(count_before, count_after)


class HubApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = ManualClock(T0)
        self.domain = DomainService(self.database, self.clock)
        self.hub = HubService(self.database, self.clock)
        _build(self.database, self.clock)
        self.headers = {"X-Actor-Id": "admin-hub"}

    def tearDown(self):
        self.database.close()

    def _call(self, method, path, body=None, headers=None):
        return route(self.domain, method, path, body, headers or self.headers)

    def test_resource_and_batch_registration(self):
        status, payload = self._call("POST", "/hub/resources", {
            "request_id": "rc2", "site_id": "s1", "resource_id": "crane-9",
            "kind": "crane", "name": "九号吊机"})
        self.assertEqual(201, status)
        self.assertEqual("crane-9", payload["resource_id"])

    def test_plan_confirm_commit_flow_over_http(self):
        status, plan = self._call("POST", "/hub/plans",
                                  {"request_id": "p1", "site_id": "s1",
                                   "valid_minutes": 600})
        self.assertEqual(201, status)
        plan_id = plan["plan_id"]
        status, _ = self._call("POST", "/hub/plans/confirm",
                               {"request_id": "cf-a", "plan_id": plan_id},
                               {"X-Actor-Id": "admin-hub"})
        self.assertEqual(200, status)
        status, _ = self._call("POST", "/hub/plans/confirm",
                               {"request_id": "cf-b", "plan_id": plan_id},
                               {"X-Actor-Id": "op-b"})
        self.assertEqual(200, status)
        # 两方承运方都确认后原子占用
        status, committed = self._call("POST", "/hub/plans/commit",
                                       {"request_id": "cm", "plan_id": plan_id})
        self.assertEqual(201, status)
        self.assertEqual("committed", committed["state"])

    def test_event_ingest_and_shipment_query(self):
        status, payload = self._call("POST", "/hub/events", {
            "request_id": "ev1", "site_id": "s1",
            "event": {"event_id": "ev-early", "event_type": "arrival.early",
                      "occurred_at": "2026-09-25T20:00:00Z", "sequence": 1,
                      "batch_id": "b1", "arrival_at": "2026-09-26T00:30:00Z"}})
        self.assertEqual(201, status)
        status, body = self._call("GET", "/hub/shipment?site_id=s1&shipment_id=sh-1", None)
        self.assertEqual(200, status)
        self.assertEqual(["box-1"], body["arrived"])

    def test_conservation_and_rate_endpoints(self):
        status, body = self._call("GET", "/hub/conservation?site_id=s1", None)
        self.assertEqual(200, status)
        self.assertEqual(1, body["shipments"]["sh-1"]["registered"])
        status, body = self._call("GET", "/hub/rate?site_id=s1&as_of=2026-09-26T02:00:00Z",
                                  None)
        self.assertEqual(200, status)
        self.assertEqual(0, body["arrived_count"])
        self.assertEqual(0.0, body["rate"])

    def test_invalid_event_returns_400(self):
        status, payload = self._call("POST", "/hub/events", {
            "request_id": "bad", "site_id": "s1",
            "event": {"event_id": "bad", "event_type": "not-a-type",
                      "occurred_at": "2026-09-25T20:00:00Z"}})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"])

    def test_writes_require_actor(self):
        status, payload = route(self.domain, "POST", "/hub/resources", {
            "request_id": "x", "site_id": "s1", "resource_id": "crane-x",
            "kind": "crane", "name": "X"}, {"X-Actor-Id": ""})
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
