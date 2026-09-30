"""换装协同应用服务：统一时间线、有期限方案、原子占用与冻结口径。

写操作一律在单条 SQLite 事务内完成；事件入库与受影响任务重排同事务，
因此提前、延误、甩箱、部分完成和设备故障要么完整生效，要么不留痕迹。
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .hub_events import format_ts, normalize_event, parse_ts
from .hub_models import PlanView, ScheduledTask, ShipmentView
from .hub_planning import (
    Interval,
    Projection,
    SIXTY_MINUTES,
    diagnose_waiting,
    earliest_pair_slot,
    load_projection,
    pending_group_tasks,
    schedule_tasks,
)
from .storage import Database

MODES = frozenset({"rail", "vessel", "vehicle"})
RESOURCE_KINDS = frozenset({"crane", "slot"})
PLAN_TTL_MINUTES = 30


class HubService:
    """在既有主体/场所边界上提供换装协同能力。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    def _now_ts(self) -> str:
        return self.clock.now().astimezone().isoformat().replace("+00:00", "Z")

    # -- 基础校验 -------------------------------------------------------

    def _actor(self, connection, actor_id: str):
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return row

    def _require_role(self, actor, *roles: str) -> None:
        if actor["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _site(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create) -> dict[str, Any]:
        request_id = str(request_id).strip()
        if not (2 <= len(request_id) <= 64):
            raise ValidationError("request_id 长度必须在 2 到 64 之间")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            result = json.loads(row["response_json"])
            result["replayed"] = True
            return result
        result = create()
        result["replayed"] = False
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
            "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, result["resource_type"],
             result["resource_id"], canonical_json({k: v for k, v in result.items()
                                                    if k != "replayed"}), self._now_ts()),
        )
        return result

    # -- 登记：资源、合同、批次、货物、封锁 ------------------------------

    def register_resource(self, *, request_id: str, actor_id: str, site_id: str,
                          resource_id: str, kind: str, name: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "resource_id": resource_id,
                   "kind": kind, "name": name}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            if kind not in RESOURCE_KINDS:
                raise ValidationError("kind 只能是 crane 或 slot")
            name = str(name).strip()
            if not name:
                raise ValidationError("name 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO hub_resources(site_id,resource_id,kind,name,created_by,created_at)"
                        " VALUES(?,?,?,?,?,?)",
                        (site_id, resource_id, kind, name, actor_id, self._now_ts()))
                except Exception as exc:
                    raise ConflictError("资源编号已存在") from exc
                append_event(conn, actor_id=actor_id, action="hub.resource_registered",
                             resource_type="hub_resource", resource_id=resource_id,
                             detail={"site_id": site_id, "kind": kind, "name": name},
                             occurred_at=self._now_ts())
                return {"resource_type": "hub_resource", "resource_id": resource_id,
                        "site_id": site_id, "kind": kind, "name": name}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_register_resource", payload=payload, create=create)

    def register_contract(self, *, request_id: str, actor_id: str, site_id: str,
                          contract_id: str, title: str, priority_rank: int) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "contract_id": contract_id,
                   "title": title, "priority_rank": priority_rank}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            if not isinstance(priority_rank, int) or isinstance(priority_rank, bool) or priority_rank < 0:
                raise ValidationError("priority_rank 必须是非负整数")
            title = str(title).strip()
            if not title:
                raise ValidationError("title 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO hub_contracts(site_id,contract_id,title,priority_rank,"
                        "created_by,created_at) VALUES(?,?,?,?,?,?)",
                        (site_id, contract_id, title, priority_rank, actor_id, self._now_ts()))
                except Exception as exc:
                    raise ConflictError("合同编号已存在") from exc
                append_event(conn, actor_id=actor_id, action="hub.contract_registered",
                             resource_type="hub_contract", resource_id=contract_id,
                             detail={"site_id": site_id, "title": title,
                                     "priority_rank": priority_rank}, occurred_at=self._now_ts())
                return {"resource_type": "hub_contract", "resource_id": contract_id,
                        "site_id": site_id, "priority_rank": priority_rank}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_register_contract", payload=payload, create=create)

    def register_batch(self, *, request_id: str, actor_id: str, site_id: str,
                       batch_id: str, mode: str, planned_arrival: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "batch_id": batch_id,
                   "mode": mode, "planned_arrival": planned_arrival}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            if mode not in MODES:
                raise ValidationError("mode 只能是 rail、vessel 或 vehicle")
            planned = format_ts(parse_ts(planned_arrival, "planned_arrival"))

            def create():
                try:
                    conn.execute(
                        "INSERT INTO hub_batches(site_id,batch_id,mode,planned_arrival,created_by,created_at)"
                        " VALUES(?,?,?,?,?,?)",
                        (site_id, batch_id, mode, planned, actor_id, self._now_ts()))
                except Exception as exc:
                    raise ConflictError("批次编号已存在") from exc
                append_event(conn, actor_id=actor_id, action="hub.batch_registered",
                             resource_type="hub_batch", resource_id=batch_id,
                             detail={"site_id": site_id, "mode": mode,
                                     "planned_arrival": planned}, occurred_at=self._now_ts())
                return {"resource_type": "hub_batch", "resource_id": batch_id,
                        "site_id": site_id, "mode": mode, "planned_arrival": planned}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_register_batch", payload=payload, create=create)

    def register_shipment(self, *, request_id: str, actor_id: str, site_id: str,
                          shipment_id: str, batch_id: str, inbound_party: str,
                          outbound_party: str, duration_minutes: int,
                          contract_id: str | None = None,
                          containers: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "shipment_id": shipment_id,
                   "batch_id": batch_id, "inbound_party": inbound_party,
                   "outbound_party": outbound_party, "duration_minutes": duration_minutes,
                   "contract_id": contract_id, "containers": containers or []}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            if conn.execute("SELECT 1 FROM hub_batches WHERE site_id=? AND batch_id=?",
                            (site_id, batch_id)).fetchone() is None:
                raise NotFoundError("批次不存在")
            if contract_id and conn.execute(
                    "SELECT 1 FROM hub_contracts WHERE site_id=? AND contract_id=?",
                    (site_id, contract_id)).fetchone() is None:
                raise NotFoundError("优先合同不存在")
            inbound_party = str(inbound_party).strip()
            outbound_party = str(outbound_party).strip()
            if not inbound_party or not outbound_party:
                raise ValidationError("交接承运方不能为空")
            if inbound_party == outbound_party:
                raise ValidationError("换装货物的交出方与接收方不能相同")
            if not isinstance(duration_minutes, int) or duration_minutes <= 0:
                raise ValidationError("duration_minutes 必须是正整数")
            specs = self._validate_containers(containers or [])

            def create():
                try:
                    conn.execute(
                        "INSERT INTO hub_shipments(site_id,shipment_id,batch_id,contract_id,"
                        "inbound_party,outbound_party,duration_minutes,created_by,created_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?)",
                        (site_id, shipment_id, batch_id, contract_id, inbound_party,
                         outbound_party, duration_minutes, actor_id, self._now_ts()))
                except Exception as exc:
                    raise ConflictError("货物编号已存在或引用无效") from exc
                for container_id, group_id, position in specs:
                    conn.execute(
                        "INSERT INTO hub_containers(site_id,shipment_id,container_id,group_id,position)"
                        " VALUES(?,?,?,?,?)",
                        (site_id, shipment_id, container_id, group_id, position))
                append_event(conn, actor_id=actor_id, action="hub.shipment_registered",
                             resource_type="hub_shipment", resource_id=shipment_id,
                             detail={"site_id": site_id, "batch_id": batch_id,
                                     "contract_id": contract_id,
                                     "container_count": len(specs)},
                             occurred_at=self._now_ts())
                return {"resource_type": "hub_shipment", "resource_id": shipment_id,
                        "site_id": site_id, "batch_id": batch_id,
                        "container_count": len(specs)}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_register_shipment", payload=payload, create=create)

    @staticmethod
    def _validate_containers(containers: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
        if not isinstance(containers, list) or not containers:
            raise ValidationError("每票货物至少登记一个集装箱")
        result: list[tuple[str, str, int]] = []
        seen: set[str] = set()
        for index, item in enumerate(containers):
            if not isinstance(item, dict):
                raise ValidationError("集装箱条目必须是对象")
            cid = str(item.get("container_id", "")).strip()
            group = str(item.get("group_id", "")).strip()
            position = item.get("position", index)
            if not cid or not group:
                raise ValidationError("container_id 与 group_id 不能为空")
            if not isinstance(position, int) or isinstance(position, bool) or position < 0:
                raise ValidationError("position 必须是非负整数")
            if cid in seen:
                raise ValidationError(f"集装箱 {cid} 重复")
            seen.add(cid)
            result.append((cid, group, position))
        return result

    def register_blockade(self, *, request_id: str, actor_id: str, site_id: str,
                          resource_id: str, start_ts: str, end_ts: str | None,
                          reason: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id, "resource_id": resource_id,
                   "start_ts": start_ts, "end_ts": end_ts, "reason": reason}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            if conn.execute("SELECT 1 FROM hub_resources WHERE site_id=? AND resource_id=?",
                            (site_id, resource_id)).fetchone() is None:
                raise NotFoundError("资源不存在")
            start = format_ts(parse_ts(start_ts, "start_ts"))
            end = format_ts(parse_ts(end_ts, "end_ts")) if end_ts else None
            if end and end <= start:
                raise ValidationError("封锁结束时间必须晚于开始时间")
            reason = str(reason).strip()
            if not reason:
                raise ValidationError("封锁原因不能为空")
            blockade_id = uuid.uuid4().hex

            def create():
                conn.execute(
                    "INSERT INTO hub_blockades(site_id,blockade_id,resource_id,start_ts,end_ts,"
                    "reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (site_id, blockade_id, resource_id, start, end, reason,
                     actor_id, self._now_ts()))
                append_event(conn, actor_id=actor_id, action="hub.blockade_registered",
                             resource_type="hub_blockade", resource_id=blockade_id,
                             detail={"site_id": site_id, "resource_id": resource_id,
                                     "start_ts": start, "end_ts": end, "reason": reason},
                             occurred_at=self._now_ts())
                return {"resource_type": "hub_blockade", "resource_id": blockade_id,
                        "site_id": site_id, "affected_resource_id": resource_id}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_register_blockade", payload=payload, create=create)

    # -- 统一时间线事件 --------------------------------------------------

    def ingest_event(self, *, request_id: str, actor_id: str, site_id: str,
                     event: dict[str, Any]) -> dict[str, Any]:
        """把承运方消息归一化后追加进统一时间线，并推动受影响任务重排。"""

        if not isinstance(event, dict):
            raise ValidationError("event 必须是对象")
        normalized = normalize_event(event)
        event_id = str(event.get("event_id", "")).strip()
        if not (2 <= len(event_id) <= 64):
            raise ValidationError("event_id 长度必须在 2 到 64 之间")
        normalized["event_id"] = event_id
        payload = {"actor_id": actor_id, "site_id": site_id, "event": normalized}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            self._validate_event_references(conn, site_id, normalized)

            existing = conn.execute(
                "SELECT payload_hash FROM hub_events WHERE event_id=?", (event_id,)).fetchone()
            if existing is not None:
                if existing["payload_hash"] != digest(normalized):
                    raise ConflictError("event_id 已被不同内容使用")
                receipt = conn.execute(
                    "SELECT response_json FROM request_receipts "
                    "WHERE action='hub_ingest_event' AND resource_id=?",
                    (event_id,)).fetchone()
                result = json.loads(receipt["response_json"]) if receipt else {
                    "resource_type": "hub_event", "resource_id": event_id, "site_id": site_id,
                    "rescheduled_plan_id": None, "released_tasks": []}
                result["replayed"] = True
                return result

            def create():
                occurred = normalized["occurred_at"]
                conn.execute(
                    "INSERT INTO hub_events(event_id,site_id,event_type,occurred_at,"
                    "payload_hash,payload_json,delivered_by,delivered_at)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (event_id, site_id, normalized["event_type"], occurred,
                     digest(normalized), canonical_json(normalized),
                     actor_id, self._now_ts()))
                # 全量折叠做因果校验：乱序消息也不能破坏单调交接与数量守恒
                projection = load_projection(conn, site_id)
                projection.conservation_report()
                released, plan_id = self._reschedule_affected(
                    conn, site_id, normalized, projection, actor_id)
                append_event(conn, actor_id=actor_id, action="hub.event_ingested",
                             resource_type="hub_event", resource_id=event_id,
                             detail={"site_id": site_id, "event_type": normalized["event_type"],
                                     "occurred_at": occurred,
                                     "rescheduled_plan_id": plan_id,
                                     "released_tasks": len(released)},
                             occurred_at=self._now_ts())
                return {"resource_type": "hub_event", "resource_id": event_id,
                        "site_id": site_id, "rescheduled_plan_id": plan_id,
                        "released_tasks": released}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_ingest_event", payload=payload, create=create)

    def _validate_event_references(self, conn, site_id: str, normalized: dict[str, Any]) -> None:
        etype = normalized["event_type"]
        if etype in ("arrival.early", "arrival.confirmed", "arrival.delayed", "arrival.skipped"):
            if conn.execute("SELECT 1 FROM hub_batches WHERE site_id=? AND batch_id=?",
                            (site_id, normalized["batch_id"])).fetchone() is None:
                raise NotFoundError("事件引用的批次不存在")
        if etype == "arrival.skipped":
            found = {row["container_id"] for row in conn.execute(
                "SELECT c.container_id FROM hub_containers c JOIN hub_shipments s "
                "ON s.site_id=c.site_id AND s.shipment_id=c.shipment_id "
                "WHERE c.site_id=? AND s.batch_id=?",
                (site_id, normalized["batch_id"]))}
            missing = [cid for cid in normalized["container_ids"] if cid not in found]
            if missing:
                raise ValidationError(f"甩箱集装箱不属于该批次: {','.join(missing)}")
        if etype == "work.partial":
            row = conn.execute(
                "SELECT 1 FROM hub_shipments WHERE site_id=? AND shipment_id=?",
                (site_id, normalized["shipment_id"])).fetchone()
            if row is None:
                raise NotFoundError("事件引用的货物不存在")
            found = {r["container_id"] for r in conn.execute(
                "SELECT container_id FROM hub_containers WHERE site_id=? AND shipment_id=?",
                (site_id, normalized["shipment_id"]))}
            missing = [cid for cid in normalized["container_ids"] if cid not in found]
            if missing:
                raise ValidationError(f"完成集装箱不属于该票货物: {','.join(missing)}")
        if etype in ("equipment.failed", "equipment.recovered"):
            row = conn.execute(
                "SELECT kind FROM hub_resources WHERE site_id=? AND resource_id=?",
                (site_id, normalized["resource_id"])).fetchone()
            if row is None:
                raise NotFoundError("事件引用的资源不存在")
            if etype == "equipment.failed" and row["kind"] != "crane":
                raise ValidationError("设备故障事件只适用于吊机")

    # -- 受影响任务重排 --------------------------------------------------

    def _committed_task_rows(self, conn, site_id: str):
        return conn.execute(
            "SELECT p.plan_id, t.task_index, t.shipment_id, t.group_key, t.container_ids_json,"
            " t.start_ts, t.end_ts, t.crane_id, t.slot_id, t.ready_at "
            "FROM hub_plan_tasks t JOIN hub_plans p ON p.plan_id=t.plan_id "
            "WHERE p.site_id=? AND p.state='committed' "
            "AND NOT EXISTS (SELECT 1 FROM hub_plan_releases r "
            "WHERE r.plan_id=t.plan_id AND r.task_index=t.task_index) "
            "ORDER BY t.plan_id, t.task_index",
            (site_id,)).fetchall()

    def _reschedule_affected(self, conn, site_id: str, normalized: dict[str, Any],
                             projection: Projection, actor_id: str
                             ) -> tuple[list[dict[str, Any]], str | None]:
        etype = normalized["event_type"]
        affected_keys: set[tuple[str, str]] = set()
        failure_window: tuple[str, str, str] | None = None

        if etype in ("arrival.early", "arrival.confirmed", "arrival.delayed"):
            # 只有“最新信息”的到达事件才推动重排，更早的预测不抢占
            if not self._is_latest_arrival(conn, site_id, normalized):
                return [], None
            for shipment_id, state in projection.shipments.items():
                if state.batch_id != normalized["batch_id"]:
                    continue
                affected_keys.update((shipment_id, group) for group in state.group_ids)
        elif etype == "arrival.skipped":
            for state in projection.shipments.values():
                if state.batch_id != normalized["batch_id"]:
                    continue
                for cid in normalized["container_ids"]:
                    box = state.containers.get(cid)
                    if box is not None:
                        affected_keys.add((state.shipment_id, box.group_id))
        elif etype == "work.partial":
            state = projection.shipments[normalized["shipment_id"]]
            for cid in normalized["container_ids"]:
                affected_keys.add((state.shipment_id, state.containers[cid].group_id))
        elif etype == "equipment.failed":
            failure_window = (normalized["resource_id"],
                              normalized["occurred_at"], normalized["end_ts"])
        else:  # equipment.recovered：故障区间在投影中自动截断，受影响的提案需要重提
            window = (normalized["resource_id"], normalized["occurred_at"],
                      normalized["occurred_at"])
            self._supersede_open_plans(conn, site_id, affected_keys, window)
            return [], None

        task_rows = self._committed_task_rows(conn, site_id)
        released: list[dict[str, Any]] = []
        predecessor_plans: list[str] = []
        f_start = parse_ts(failure_window[1]) if failure_window else None
        f_end = parse_ts(failure_window[2]) if failure_window else None
        for row in task_rows:
            hit = (row["shipment_id"], row["group_key"]) in affected_keys
            if not hit and failure_window is not None and (
                    row["crane_id"] == failure_window[0] or row["slot_id"] == failure_window[0]):
                t_start = parse_ts(row["start_ts"])
                t_end = parse_ts(row["end_ts"])
                hit = t_start < f_end and t_end > f_start
            if not hit:
                continue
            state = projection.shipments.get(row["shipment_id"])
            if state is None:
                continue
            ids = set(json.loads(row["container_ids_json"]))
            if all((box := state.containers.get(cid)) is not None
                   and box.transferred_at is not None for cid in ids):
                continue  # 已全部交接的任务不重排
            released.append({"plan_id": row["plan_id"], "task_index": row["task_index"],
                             "shipment_id": row["shipment_id"], "group_key": row["group_key"]})
            predecessor_plans.append(row["plan_id"])
            affected_keys.add((row["shipment_id"], row["group_key"]))

        # 失效引用同一范围的未提交提案（提案不占资源，无需释放）
        stale_plans = self._supersede_open_plans(conn, site_id, affected_keys, failure_window)

        if not released and not stale_plans:
            return [], None

        reason = f"event:{etype}"
        now_text = self._now_ts()
        for item in released:
            conn.execute(
                "DELETE FROM hub_holds WHERE site_id=? AND plan_id=? AND shipment_id=? AND group_key=?",
                (site_id, item["plan_id"], item["shipment_id"], item["group_key"]))
            conn.execute(
                "INSERT INTO hub_plan_releases(plan_id,task_index,site_id,reason,released_at)"
                " VALUES(?,?,?,?,?)",
                (item["plan_id"], item["task_index"], site_id, reason, now_text))

        # 在释放后的时间线上只为受影响箱组重新提案
        projection = load_projection(conn, site_id)
        candidates = [task for task in pending_group_tasks(projection)
                      if (task.shipment.shipment_id, task.group_id) in affected_keys]
        if not candidates:
            return released, None
        scheduled = schedule_tasks(projection, candidates)
        plan_id = f"plan-{uuid.uuid4().hex[:12]}"
        expires = format_ts(self.clock.now() + timedelta(minutes=PLAN_TTL_MINUTES))
        conn.execute(
            "INSERT INTO hub_plans(site_id,plan_id,supersedes,state,expires_at,committed_at,"
            "created_by,created_at) VALUES(?,?,?, 'open',?,NULL,?,?)",
            (site_id, plan_id, predecessor_plans[-1] if predecessor_plans else stale_plans[-1],
             expires, actor_id, self._now_ts()))
        self._insert_tasks(conn, plan_id, scheduled)
        append_event(conn, actor_id=actor_id, action="hub.plan_rescheduled",
                     resource_type="hub_plan", resource_id=plan_id,
                     detail={"site_id": site_id,
                             "supersedes": predecessor_plans[-1] if predecessor_plans
                             else stale_plans[-1],
                             "released": len(released), "tasks": len(scheduled),
                             "trigger": etype}, occurred_at=self._now_ts())
        return released, plan_id

    def _is_latest_arrival(self, conn, site_id: str, normalized: dict[str, Any]) -> bool:
        """判断刚入库的到达事件是否为该批次最新一条（排除事件自身）。"""

        rows = conn.execute(
            "SELECT event_id, occurred_at, json_extract(payload_json,'$.sequence') AS seq "
            "FROM hub_events WHERE site_id=? AND event_id<>? "
            "AND event_type IN ('arrival.early','arrival.confirmed','arrival.delayed') "
            "AND json_extract(payload_json,'$.batch_id')=?",
            (site_id, normalized["event_id"], normalized["batch_id"])).fetchall()
        new_key = (normalized["occurred_at"], normalized["sequence"], normalized["event_id"])
        return all((r["occurred_at"], r["seq"], r["event_id"]) <= new_key for r in rows)

    def _supersede_open_plans(self, conn, site_id: str,
                              affected_keys: set[tuple[str, str]],
                              failure_window: tuple[str, str, str] | None) -> list[str]:
        rows = conn.execute(
            "SELECT p.plan_id, t.shipment_id, t.group_key, t.crane_id, t.slot_id "
            "FROM hub_plans p JOIN hub_plan_tasks t ON t.plan_id=p.plan_id "
            "WHERE p.site_id=? AND p.state='open'", (site_id,)).fetchall()
        stale = set()
        for row in rows:
            if (row["shipment_id"], row["group_key"]) in affected_keys:
                stale.add(row["plan_id"])
            elif failure_window is not None and (
                    row["crane_id"] == failure_window[0]
                    or row["slot_id"] == failure_window[0]):
                stale.add(row["plan_id"])
        for plan_id in stale:
            conn.execute("UPDATE hub_plans SET state='superseded' WHERE plan_id=?", (plan_id,))
        return sorted(stale)

    # -- 有期限方案：提案、承运方确认、原子提交 ----------------------------

    def create_plan(self, *, request_id: str, actor_id: str, site_id: str,
                    shipment_ids: list[str] | None = None,
                    valid_minutes: int = PLAN_TTL_MINUTES) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "site_id": site_id,
                   "shipment_ids": shipment_ids, "valid_minutes": valid_minutes}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            self._site(conn, site_id)
            if not isinstance(valid_minutes, int) or not (1 <= valid_minutes <= 1440):
                raise ValidationError("valid_minutes 必须在 1 到 1440 之间")
            self._lapse_expired(conn, site_id)
            projection = load_projection(conn, site_id)
            projection.conservation_report()
            tasks = pending_group_tasks(projection)
            if shipment_ids is not None:
                wanted = set(shipment_ids)
                tasks = [task for task in tasks
                         if task.shipment.shipment_id in wanted]
                if not tasks:
                    raise ValidationError("选定货物没有待安排的箱组任务")
            if not tasks:
                raise ConflictError("当前没有待排任务")
            scheduled = schedule_tasks(projection, tasks)
            plan_id = f"plan-{uuid.uuid4().hex[:12]}"
            expires = format_ts(self.clock.now() + timedelta(minutes=valid_minutes))

            def create():
                conn.execute(
                    "INSERT INTO hub_plans(site_id,plan_id,supersedes,state,expires_at,"
                    "committed_at,created_by,created_at) VALUES(?,?,NULL,'open',?,NULL,?,?)",
                    (site_id, plan_id, expires, actor_id, self._now_ts()))
                self._insert_tasks(conn, plan_id, scheduled)
                append_event(conn, actor_id=actor_id, action="hub.plan_proposed",
                             resource_type="hub_plan", resource_id=plan_id,
                             detail={"site_id": site_id, "tasks": len(scheduled),
                                     "expires_at": expires}, occurred_at=self._now_ts())
                return {"resource_type": "hub_plan", "resource_id": plan_id,
                        "plan_id": plan_id,
                        "site_id": site_id, "state": "open", "expires_at": expires,
                        "tasks": scheduled}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_create_plan", payload=payload, create=create)

    @staticmethod
    def _insert_tasks(conn, plan_id: str, scheduled: list[dict[str, Any]]) -> None:
        for item in scheduled:
            conn.execute(
                "INSERT INTO hub_plan_tasks(plan_id,task_index,shipment_id,group_key,"
                "container_ids_json,ready_at,start_ts,end_ts,crane_id,slot_id)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (plan_id, item["task_index"], item["shipment_id"], item["group_key"],
                 canonical_json(item["container_ids"]), item["ready_at"],
                 item["start_ts"], item["end_ts"], item["crane_id"], item["slot_id"]))

    def _lapse_expired(self, conn, site_id: str) -> None:
        now_text = self._now_ts()
        conn.execute(
            "UPDATE hub_plans SET state='lapsed' WHERE site_id=? AND state='open' AND expires_at<=?",
            (site_id, now_text))

    def _load_plan_row(self, conn, plan_id: str):
        row = conn.execute("SELECT * FROM hub_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError("方案不存在")
        return row

    def confirm_plan(self, *, request_id: str, actor_id: str, plan_id: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "plan_id": plan_id}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            plan = self._load_plan_row(conn, plan_id)
            site_id = plan["site_id"]
            self._lapse_expired(conn, site_id)
            plan = self._load_plan_row(conn, plan_id)
            if plan["state"] != "open":
                raise ConflictError(f"方案当前状态为 {plan['state']}，不能确认")
            parties = self._plan_parties(conn, plan_id)
            if actor["organization_id"] not in parties:
                raise PermissionDenied("只有方案涉及的承运方可以确认")

            def create():
                conn.execute(
                    "INSERT INTO hub_plan_confirms(plan_id,party,actor_id,confirmed_at)"
                    " VALUES(?,?,?,?) ON CONFLICT(plan_id,party) DO UPDATE SET "
                    "actor_id=excluded.actor_id, confirmed_at=excluded.confirmed_at",
                    (plan_id, actor["organization_id"], actor_id, self._now_ts()))
                append_event(conn, actor_id=actor_id, action="hub.plan_confirmed",
                             resource_type="hub_plan", resource_id=plan_id,
                             detail={"site_id": site_id,
                                     "party": actor["organization_id"]},
                             occurred_at=self._now_ts())
                view = self._plan_view(conn, plan_id)
                return {"resource_type": "hub_plan", "resource_id": plan_id,
                        "site_id": site_id, "state": view.state,
                        "confirmations": list(view.confirmations),
                        "required_parties": sorted(parties)}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_confirm_plan", payload=payload, create=create)

    def _plan_parties(self, conn, plan_id: str) -> set[str]:
        rows = conn.execute(
            "SELECT DISTINCT s.inbound_party, s.outbound_party "
            "FROM hub_plan_tasks t JOIN hub_shipments s ON s.shipment_id=t.shipment_id "
            "WHERE t.plan_id=?", (plan_id,)).fetchall()
        return {p for row in rows for p in (row["inbound_party"], row["outbound_party"])}

    def commit_plan(self, *, request_id: str, actor_id: str, plan_id: str) -> dict[str, Any]:
        """各承运方确认齐备后原子占用吊机与堆位。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator")
            plan = self._load_plan_row(conn, plan_id)
            site_id = plan["site_id"]
            self._lapse_expired(conn, site_id)
            plan = self._load_plan_row(conn, plan_id)
            if plan["state"] == "committed":
                view = self._plan_view(conn, plan_id)
                return {"resource_type": "hub_plan", "resource_id": plan_id,
                        "site_id": site_id, "state": "committed", "replayed": True,
                        "confirmations": list(view.confirmations)}
            if plan["state"] != "open":
                raise ConflictError(f"方案当前状态为 {plan['state']}，不能占用")
            required = self._plan_parties(conn, plan_id)
            confirmed = {row["party"] for row in conn.execute(
                "SELECT party FROM hub_plan_confirms WHERE plan_id=?", (plan_id,))}
            missing = sorted(required - confirmed)
            if missing:
                raise ConflictError(f"承运方尚未全部确认，缺少: {','.join(missing)}")
            task_rows = conn.execute(
                "SELECT * FROM hub_plan_tasks WHERE plan_id=? ORDER BY task_index",
                (plan_id,)).fetchall()
            # 提交前在最新时间线上复验可行性，过期/被抢占的提案不得落锁。
            # 方案内任务的互相占用需要作为额外忙碌区间纳入，否则多任务方案
            # 会被误判为可以更早开始。
            projection = load_projection(conn, site_id)
            for row in task_rows:
                # 复验当前任务时排除它自己的预约，只让同方案的其他任务形成约束
                reserved: dict[str, list[Interval]] = {}
                for other in task_rows:
                    if other["task_index"] == row["task_index"]:
                        continue
                    for resource_id in (other["crane_id"], other["slot_id"]):
                        reserved.setdefault(resource_id, []).append(Interval(
                            parse_ts(other["start_ts"]), parse_ts(other["end_ts"]),
                            "plan_check", other["shipment_id"]))
                duration_minutes = projection.shipments[row["shipment_id"]].duration_minutes
                start = earliest_pair_slot(
                    projection, parse_ts(row["ready_at"]),
                    timedelta(minutes=duration_minutes),
                    row["crane_id"], row["slot_id"], reserved)
                if start is None or start != parse_ts(row["start_ts"]):
                    raise ConflictError("方案已被时间线变化抢占，请重新提案")

            def create():
                for row in task_rows:
                    for resource_id in (row["crane_id"], row["slot_id"]):
                        conn.execute(
                            "INSERT INTO hub_holds(site_id,plan_id,shipment_id,group_key,"
                            "resource_id,start_ts,end_ts) VALUES(?,?,?,?,?,?,?)",
                            (site_id, plan_id, row["shipment_id"], row["group_key"],
                             resource_id, row["start_ts"], row["end_ts"]))
                conn.execute(
                    "UPDATE hub_plans SET state='committed', committed_at=? WHERE plan_id=?",
                    (self._now_ts(), plan_id))
                append_event(conn, actor_id=actor_id, action="hub.plan_committed",
                             resource_type="hub_plan", resource_id=plan_id,
                             detail={"site_id": site_id, "tasks": len(task_rows)},
                             occurred_at=self._now_ts())
                return {"resource_type": "hub_plan", "resource_id": plan_id,
                        "site_id": site_id, "state": "committed",
                        "tasks": len(task_rows)}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_commit_plan", payload=payload, create=create)

    def _plan_view(self, conn, plan_id: str) -> PlanView:
        plan = self._load_plan_row(conn, plan_id)
        releases = {(r["plan_id"], r["task_index"]): r
                    for r in conn.execute("SELECT * FROM hub_plan_releases")}
        tasks = []
        for row in conn.execute(
                "SELECT * FROM hub_plan_tasks WHERE plan_id=? ORDER BY task_index", (plan_id,)):
            release = releases.get((plan_id, row["task_index"]))
            tasks.append(ScheduledTask(
                row["task_index"], row["shipment_id"], row["group_key"],
                tuple(json.loads(row["container_ids_json"])),
                row["ready_at"], row["start_ts"], row["end_ts"],
                row["crane_id"], row["slot_id"]))
        confirms = tuple(sorted(r["party"] for r in conn.execute(
            "SELECT party FROM hub_plan_confirms WHERE plan_id=?", (plan_id,))))
        return PlanView(plan_id, plan["site_id"], plan["state"], plan["expires_at"],
                        plan["committed_at"], plan["supersedes"], confirms, tuple(tasks))

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        with self.database.transaction() as conn:
            plan = self._load_plan_row(conn, plan_id)
            self._lapse_expired(conn, plan["site_id"])
            view = self._plan_view(conn, plan_id)
        return self._plan_dict(view)

    @staticmethod
    def _plan_dict(view: PlanView) -> dict[str, Any]:
        required: set[str] = set()
        tasks = []
        for task in view.tasks:
            tasks.append({
                "task_index": task.task_index,
                "shipment_id": task.shipment_id,
                "group_key": task.group_key,
                "container_ids": list(task.container_ids),
                "ready_at": task.ready_at,
                "start_ts": task.start_ts,
                "end_ts": task.end_ts,
                "crane_id": task.crane_id,
                "slot_id": task.slot_id,
            })
        return {
            "plan_id": view.plan_id,
            "site_id": view.site_id,
            "state": view.state,
            "expires_at": view.expires_at,
            "committed_at": view.committed_at,
            "supersedes": view.supersedes,
            "confirmations": list(view.confirmations),
            "tasks": tasks,
        }

    # -- 查询：货物视图、时间线、守恒、换装率 -----------------------------

    def list_shipments(self, site_id: str) -> list[dict[str, Any]]:
        with self.database.transaction() as conn:
            self._site(conn, site_id)
            projection = load_projection(conn, site_id)
            now = self.clock.now()
            result = [self._shipment_dict(conn, projection, shipment_id, now)
                      for shipment_id in sorted(projection.shipments)]
        return result

    def get_shipment(self, site_id: str, shipment_id: str) -> dict[str, Any]:
        with self.database.transaction() as conn:
            self._site(conn, site_id)
            projection = load_projection(conn, site_id)
            if shipment_id not in projection.shipments:
                raise NotFoundError("货物不存在")
            result = self._shipment_dict(conn, projection, shipment_id, self.clock.now())
        return result

    def _shipment_dict(self, conn, projection: Projection, shipment_id: str, now) -> dict[str, Any]:
        state = projection.shipments[shipment_id]
        ordered = sorted(state.containers.values(), key=lambda b: (b.position, b.container_id))
        all_ids = tuple(b.container_id for b in ordered)
        arrived = tuple(b.container_id for b in ordered if b.arrival_at is not None)
        transferred = tuple(b.container_id for b in ordered if b.transferred_at is not None)
        skipped = tuple(b.container_id for b in ordered if b.skipped)
        pending = [b for b in ordered if not b.skipped and b.transferred_at is None]

        if not arrived and not transferred:
            stage = "registered"
            custodian = state.inbound_party
        elif len(transferred) == len([b for b in ordered if not b.skipped]):
            stage = "completed"
            custodian = state.outbound_party
        elif transferred:
            stage = "partial_handover"
            custodian = f"{state.inbound_party}>{state.outbound_party}"
        else:
            stage = "arrived_pending"
            custodian = state.inbound_party

        waiting: str | None = None
        plan_id: str | None = None
        start_ts = end_ts = None
        if pending:
            groups = {b.group_id for b in pending}
            group_id = sorted(groups)[0]
            target = next((task for task in pending_group_tasks(projection)
                           if task.shipment.shipment_id == shipment_id and task.group_id == group_id),
                          None)
            if target is not None:
                waiting = diagnose_waiting(projection, target, now)
            placement = conn.execute(
                "SELECT p.plan_id, p.state, t.start_ts, t.end_ts FROM hub_plan_tasks t "
                "JOIN hub_plans p ON p.plan_id=t.plan_id "
                "WHERE t.shipment_id=? AND p.state IN ('open','committed') "
                "AND NOT EXISTS (SELECT 1 FROM hub_plan_releases r "
                "WHERE r.plan_id=t.plan_id AND r.task_index=t.task_index) "
                "ORDER BY CASE p.state WHEN 'committed' THEN 0 ELSE 1 END, t.start_ts LIMIT 1",
                (shipment_id,)).fetchone()
            if placement is not None:
                plan_id = placement["plan_id"]
                start_ts = placement["start_ts"]
                end_ts = placement["end_ts"]

        within_one_hour: bool | None = None
        arrival = projection.batch_arrival.get(state.batch_id)
        if transferred and arrival is not None:
            within_one_hour = all(
                state.containers[cid].transferred_at - arrival <= SIXTY_MINUTES
                for cid in transferred)

        data = ShipmentView(
            shipment_id, state.batch_id, state.contract_id,
            state.inbound_party, state.outbound_party, all_ids, arrived,
            transferred, stage, custodian, waiting, plan_id, start_ts, end_ts,
            within_one_hour).__dict__
        data["container_ids"] = list(data["container_ids"])
        data["arrived"] = list(data["arrived"])
        data["transferred"] = list(data["transferred"])
        data["skipped"] = list(skipped)
        return data

    def timeline(self, site_id: str) -> list[dict[str, Any]]:
        with self.database.transaction() as conn:
            self._site(conn, site_id)
            rows = conn.execute(
                "SELECT event_id,event_type,occurred_at,payload_json,delivered_by,delivered_at "
                "FROM hub_events WHERE site_id=? "
                "ORDER BY occurred_at, json_extract(payload_json,'$.sequence'), event_id",
                (site_id,)).fetchall()
        return [{"event_id": r["event_id"], "event_type": r["event_type"],
                 "occurred_at": r["occurred_at"], "payload": json.loads(r["payload_json"]),
                 "delivered_by": r["delivered_by"], "delivered_at": r["delivered_at"]}
                for r in rows]

    def conservation(self, site_id: str) -> dict[str, Any]:
        with self.database.transaction() as conn:
            self._site(conn, site_id)
            projection = load_projection(conn, site_id)
            report = projection.conservation_report()
        return {"site_id": site_id, "shipments": report}

    def compute_rate(self, site_id: str, as_of: str | None = None,
                     _conn=None) -> dict[str, Any]:
        """计算一小时换装率：分母为截至时点已实际到达的箱，甩箱永不计入。"""

        moment = parse_ts(as_of) if as_of else self.clock.now()
        if _conn is not None:
            self._site(_conn, site_id)
            projection = load_projection(_conn, site_id)
            projection.conservation_report()
            return self._rate_payload(site_id, moment, projection)
        with self.database.transaction() as conn:
            self._site(conn, site_id)
            projection = load_projection(conn, site_id)
            projection.conservation_report()
            return self._rate_payload(site_id, moment, projection)

    @staticmethod
    def _rate_payload(site_id: str, moment, projection: Projection) -> dict[str, Any]:
        arrived_count = within_count = transferred_count = 0
        shipment_detail = {}
        for shipment_id, state in projection.shipments.items():
            s_arrived = s_done = s_within = 0
            for box in state.containers.values():
                if box.skipped or box.arrival_at is None or box.arrival_at > moment:
                    continue
                s_arrived += 1
                if box.transferred_at is not None and box.transferred_at <= moment:
                    s_done += 1
                    if box.transferred_at - box.arrival_at <= SIXTY_MINUTES:
                        s_within += 1
            arrived_count += s_arrived
            transferred_count += s_done
            within_count += s_within
            shipment_detail[shipment_id] = {
                "arrived": s_arrived, "transferred": s_done, "within_60m": s_within}
        rate = (within_count / arrived_count) if arrived_count else 0.0
        return {"site_id": site_id, "as_of": format_ts(moment),
                "arrived_count": arrived_count, "transferred_count": transferred_count,
                "within_60m_count": within_count, "rate": round(rate, 6),
                "shipments": shipment_detail}

    def freeze_rate(self, *, request_id: str, actor_id: str, site_id: str,
                    as_of: str) -> dict[str, Any]:
        """把某一时点的换装率口径固化，供考核追溯，之后不可修改。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "as_of": as_of}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require_role(actor, "admin", "operator", "reviewer")
            self._site(conn, site_id)
            as_of_text = format_ts(parse_ts(as_of, "as_of"))
            rate_data = self.compute_rate(site_id, as_of_text, _conn=conn)
            freeze_id = f"freeze-{uuid.uuid4().hex[:12]}"

            def create():
                try:
                    conn.execute(
                        "INSERT INTO hub_rate_freezes(site_id,freeze_id,as_of,arrived_count,"
                        "transferred_count,within_60m_count,rate,detail_json,created_by,created_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (site_id, freeze_id, as_of_text, rate_data["arrived_count"],
                         rate_data["transferred_count"], rate_data["within_60m_count"],
                         rate_data["rate"], canonical_json(rate_data["shipments"]),
                         actor_id, self._now_ts()))
                except Exception as exc:
                    raise ConflictError("该时点的换装率已经冻结") from exc
                append_event(conn, actor_id=actor_id, action="hub.rate_frozen",
                             resource_type="hub_rate_freeze", resource_id=freeze_id,
                             detail={"site_id": site_id, "as_of": as_of_text,
                                     "rate": rate_data["rate"]}, occurred_at=self._now_ts())
                return {"resource_type": "hub_rate_freeze", "resource_id": freeze_id,
                        **rate_data, "freeze_id": freeze_id}

            return self._idempotent(conn, request_id=request_id,
                                    action="hub_freeze_rate", payload=payload, create=create)

    def list_freezes(self, site_id: str) -> list[dict[str, Any]]:
        with self.database.transaction() as conn:
            self._site(conn, site_id)
            rows = conn.execute(
                "SELECT * FROM hub_rate_freezes WHERE site_id=? ORDER BY as_of", (site_id,))
            result = [{
                "freeze_id": r["freeze_id"], "site_id": r["site_id"], "as_of": r["as_of"],
                "arrived_count": r["arrived_count"], "transferred_count": r["transferred_count"],
                "within_60m_count": r["within_60m_count"], "rate": r["rate"],
            } for r in rows]
        return result
