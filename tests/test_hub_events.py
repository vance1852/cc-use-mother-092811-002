"""事件折叠的确定性、乱序安全、重放一致与数量守恒测试。"""

import unittest

from transport_coordination.errors import ConflictError

from hub_support import build_hub


def make_shipment(hub, groups=(("G1", 10),), inbound="RAIL", outbound="ROAD"):
    hub.register_shipment(
        request_id="ship-1", actor_id="mgr", shipment_id="SHIP1", site_id="S1",
        inbound_party=inbound, outbound_party=outbound,
        groups=[{"group_id": gid, "quantity": qty, "work_minutes": 20}
                for gid, qty in groups])
    hub.register_batch(
        request_id="batch-1", actor_id="mgr", batch_id="B1", site_id="S1",
        mode="rail", direction="inbound", planned_arrival="2026-09-30T08:00Z",
        manifest=[{"group_id": gid, "quantity": qty} for gid, qty in groups])
    hub.register_batch(
        request_id="batch-2", actor_id="mgr", batch_id="B2", site_id="S1",
        mode="road", direction="outbound", planned_arrival="2026-09-30T10:00Z",
        planned_departure="2026-09-30T10:30Z",
        manifest=[{"group_id": gid, "quantity": qty} for gid, qty in groups])


def group_view(hub, gid="G1"):
    view = hub.get_shipment_view("SHIP1")
    return next(g for g in view["groups"] if g["group_id"] == gid)


def snapshot(hub):
    """生成与投递顺序无关的可比较状态快照。"""
    view = hub.get_shipment_view("SHIP1")
    timeline = [(e["kind"], e.get("resource_id") or e.get("group_id"),
                 e.get("start_ts") or e.get("at_ts"), e.get("status"),
                 e.get("release_reason")) for e in hub.get_timeline("S1")["events"]]
    return view, sorted(timeline, key=str)


class FoldingTest(unittest.TestCase):
    def setUp(self):
        self.database, self.svc, self.hub, self.clock = build_hub()
        make_shipment(self.hub)

    def tearDown(self):
        self.database.close()

    def _arrival_events(self, order):
        events = [
            ("ev-delay", "E-ARR", "arrival", "2026-09-30T08:40Z",
             {"site_id": "S1", "batch_id": "B1", "actual_arrival": "2026-09-30T08:40Z"}),
            ("ev-aq", "E-AQ", "arrival_qty", "2026-09-30T08:41Z",
             {"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": 7}),
            ("ev-drop", "E-DROP", "drop_qty", "2026-09-30T08:41Z",
             {"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": 3}),
        ]
        for index in order:
            req, eid, etype, ts, payload = events[index]
            self.hub.ingest_event(request_id=req, actor_id="railop", event_id=eid,
                                  event_type=etype, occurred_at=ts, payload=payload)

    def test_out_of_order_events_fold_by_occurred_time(self):
        # 先到甩箱、再到到达量、最后到列车到达：结果必须与正序相同。
        self._arrival_events([2, 1, 0])
        view_reversed, timeline_reversed = snapshot(self.hub)
        g = next(x for x in view_reversed["groups"] if x["group_id"] == "G1")
        self.assertEqual(7, g["arrived_qty"])
        self.assertEqual(3, g["dropped_qty"])
        # 7 箱已到达待换装，3 箱甩箱；剩余作业量为 7。
        self.assertEqual(7, g["remaining_qty"])
        self.assertEqual("2026-09-30T08:40Z", g["ready_at"])

        database2, _svc2, hub2, _clock2 = build_hub()
        make_shipment(hub2)
        events = [
            ("ev-delay", "E-ARR", "arrival", "2026-09-30T08:40Z",
             {"site_id": "S1", "batch_id": "B1", "actual_arrival": "2026-09-30T08:40Z"}),
            ("ev-aq", "E-AQ", "arrival_qty", "2026-09-30T08:41Z",
             {"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": 7}),
            ("ev-drop", "E-DROP", "drop_qty", "2026-09-30T08:41Z",
             {"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": 3}),
        ]
        for req, eid, etype, ts, payload in events:
            hub2.ingest_event(request_id=req, actor_id="railop", event_id=eid,
                              event_type=etype, occurred_at=ts, payload=payload)
        view_forward, timeline_forward = snapshot(hub2)
        self.assertEqual(_canonical(view_reversed), _canonical(view_forward))
        self.assertEqual(timeline_reversed, timeline_forward)
        database2.close()

    def test_same_event_replays_original_result(self):
        payload = {"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": 6}
        first = self.hub.ingest_event(request_id="r1", actor_id="railop", event_id="E-AQ",
                                      event_type="arrival_qty", occurred_at="2026-09-30T08:05Z",
                                      payload=payload)
        self.assertFalse(first["replayed"])
        second = self.hub.ingest_event(request_id="r1", actor_id="railop", event_id="E-AQ",
                                       event_type="arrival_qty", occurred_at="2026-09-30T08:05Z",
                                       payload=payload)
        self.assertTrue(second["replayed"])
        # 不同 request_id 但同一自然 event_id 仍是重放。
        third = self.hub.ingest_event(request_id="r1-again", actor_id="railop", event_id="E-AQ",
                                      event_type="arrival_qty", occurred_at="2026-09-30T08:05Z",
                                      payload=payload)
        self.assertTrue(third["replayed"])
        self.assertEqual(6, group_view(self.hub)["arrived_qty"])

    def test_same_event_id_with_changed_payload_conflicts(self):
        payload = {"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": 6}
        self.hub.ingest_event(request_id="r1", actor_id="railop", event_id="E-AQ",
                              event_type="arrival_qty", occurred_at="2026-09-30T08:05Z",
                              payload=payload)
        with self.assertRaises(ConflictError):
            self.hub.ingest_event(request_id="r2", actor_id="railop", event_id="E-AQ",
                                  event_type="arrival_qty", occurred_at="2026-09-30T08:05Z",
                                  payload={**payload, "quantity": 8})

    def test_cumulative_arrival_is_monotone_and_capped_by_manifest(self):
        for qty in (4, 4, 6, 10, 12):
            self.hub.ingest_event(
                request_id=f"r-{qty}-{qty}", actor_id="railop",
                event_id=f"E-AQ-{qty}", event_type="arrival_qty",
                occurred_at="2026-09-30T08:05Z",
                payload={"site_id": "S1", "batch_id": "B1", "group_id": "G1", "quantity": qty})
        g = group_view(self.hub)
        # 取累计最大值 12，但清单只有 10：守恒钳制为 10，不产生超收。
        self.assertEqual(10, g["arrived_qty"])

    def test_handover_never_rolls_back_and_cannot_exceed_arrival(self):
        self.hub.ingest_event(request_id="arr", actor_id="railop", event_id="E-ARR",
                              event_type="arrival", occurred_at="2026-09-30T08:00Z",
                              payload={"site_id": "S1", "batch_id": "B1",
                                       "actual_arrival": "2026-09-30T08:00Z"})
        self.hub.ingest_event(request_id="aq", actor_id="railop", event_id="E-AQ",
                              event_type="arrival_qty", occurred_at="2026-09-30T08:01Z",
                              payload={"site_id": "S1", "batch_id": "B1", "group_id": "G1",
                                       "quantity": 10})
        # 进站交接 10。
        self.hub.ingest_event(request_id="hi", actor_id="railop", event_id="E-HI",
                              event_type="handover", occurred_at="2026-09-30T08:05Z",
                              payload={"site_id": "S1", "shipment_id": "SHIP1", "group_id": "G1",
                                       "stage": "inbound", "quantity": 10,
                                       "at_ts": "2026-09-30T08:05Z"})
        # 出站交接 4（部分完成）。
        self.hub.ingest_event(request_id="ho1", actor_id="roadop", event_id="E-HO1",
                              event_type="handover", occurred_at="2026-09-30T08:30Z",
                              payload={"site_id": "S1", "shipment_id": "SHIP1", "group_id": "G1",
                                       "stage": "outbound", "quantity": 4,
                                       "at_ts": "2026-09-30T08:30Z"})
        # 重复提交同一条出站交接：仍然是 4，绝不翻倍。
        replay = self.hub.ingest_event(request_id="ho1", actor_id="roadop", event_id="E-HO1",
                                       event_type="handover", occurred_at="2026-09-30T08:30Z",
                                       payload={"site_id": "S1", "shipment_id": "SHIP1",
                                                "group_id": "G1", "stage": "outbound",
                                                "quantity": 4, "at_ts": "2026-09-30T08:30Z"})
        self.assertTrue(replay["replayed"])
        g = group_view(self.hub)
        self.assertEqual(4, g["outbound_handed_qty"])
        self.assertEqual(6, g["remaining_qty"])
        self.assertEqual("HUB", g["responsibility"])

    def test_recovered_event_before_fault_event_still_unblocks(self):
        # 乱序：恢复消息先到，故障消息后到，区间仍然正确闭合。
        self.hub.ingest_event(request_id="rec", actor_id="mgr", event_id="E-REC",
                              event_type="equipment_recovered", occurred_at="2026-09-30T09:10Z",
                              payload={"site_id": "S1", "resource_id": "CR1",
                                       "fault_event_id": "E-FAULT",
                                       "end_ts": "2026-09-30T09:10Z"})
        self.hub.ingest_event(request_id="flt", actor_id="mgr", event_id="E-FAULT",
                              event_type="equipment_fault", occurred_at="2026-09-30T09:00Z",
                              payload={"site_id": "S1", "resource_id": "CR1",
                                       "start_ts": "2026-09-30T09:00Z",
                                       "reason": "故障"})
        state_outages = None
        timeline = self.hub.get_timeline("S1")["events"]
        outages = [e for e in timeline if e["kind"] == "equipment_outage"]
        self.assertEqual(1, len(outages))
        self.assertEqual("2026-09-30T09:00Z", outages[0]["start_ts"])
        self.assertEqual("2026-09-30T09:10Z", outages[0]["end_ts"])


def _canonical(value):
    import json
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


if __name__ == "__main__":
    unittest.main()
