"""多式联换装协同核心：统一时间线、两阶段承诺、确定性事件折叠与冻结考核。

设计要点：

* 列车/船舶/车辆以“批次”登记，批次清单把票（shipment）的箱组（group）挂到
  到达/出发批次上；吊机、堆位是可占用的作业资源，封锁窗口与设备故障同处一条时间线。
* 所有动态变化（提前、延误、甩箱、交接、部分完成、设备故障）都是只追加的事件。
  折叠时严格按 ``(occurred_at, event_id)`` 的规范顺序进行，因此乱序投递、重复重放
  得到同一结果；到达量、甩箱量采用累计最大值（单调不降），交接流水只追加，
  已交接货物不可能回退到“未到达”。
* 方案（plan）只给出有期限的分配建议，不占用任何能力；各承运方逐一确认，
  在全部确认的瞬间于单个事务内重验冲突并原子写入占用，任一冲突则整单拒绝、零占用。
* 故障、封锁、延误、甩箱、部分完成只会释放受影响的未来占用并把任务推回复排，
  历史占用与交接记录保持不动。
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .models import Actor
from .storage import Database
from .timeutil import add_minutes, format_ts, overlaps, parse_minute, parse_ts

RESOURCE_KINDS = frozenset({"crane", "slot"})
MODES = frozenset({"rail", "water", "road"})
DIRECTIONS = frozenset({"inbound", "outbound"})
EVENT_TYPES = frozenset({
    "arrival", "arrival_qty", "drop_qty", "batch_departure",
    "equipment_fault", "equipment_recovered", "handover",
})
HUB_PARTY = "HUB"
# 冻结考核口径：自进站交接时刻起 60 分钟内完成出站交接视为达标。
ONE_HOUR_MINUTES = 60


class HubService:
    """提供枢纽换装协同的登记、事件、计划、占用、考核与查询能力。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 基础工具

    def _now(self) -> str:
        return self.clock.now().isoformat(timespec="minutes").replace("+00:00", "Z")

    def _now_dt(self):
        return self.clock.now()

    def _actor(self, connection, actor_id: str) -> Actor:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        actor = Actor(row["actor_id"], row["display_name"], row["role"],
                      row["organization_id"], bool(row["active"]))
        if not actor.active:
            raise PermissionDenied("操作者已停用")
        return actor

    def _require(self, actor: Actor, *roles: str) -> None:
        if actor.role not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _site(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _org_exists(self, connection, org_id: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM organizations WHERE organization_id=?", (org_id,)
        ).fetchone() is not None

    def _receipt(self, connection, *, request_id: str, action: str, payload: dict[str, Any],
                 create) -> dict[str, Any]:
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return {**json.loads(row["response_json"]), "replayed": True}
        result = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, result["resource_type"], result["resource_id"],
             canonical_json(result), self._now()),
        )
        return {**result, "replayed": result.pop("replayed", False)}

    # ------------------------------------------------------------------ 基础登记

    def register_resource(self, *, request_id: str, actor_id: str, resource_id: str,
                          site_id: str, kind: str, name: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "resource_id": resource_id, "site_id": site_id,
                   "kind": kind, "name": name}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            self._site(conn, site_id)
            if kind not in RESOURCE_KINDS:
                raise ValidationError("kind 必须是 crane 或 slot")
            name = str(name).strip()
            if not name:
                raise ValidationError("name 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO hub_resources(resource_id,site_id,kind,name,active,created_at) "
                        "VALUES(?,?,?,?,1,?)",
                        (resource_id, site_id, kind, name, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("资源编号已经存在") from exc
                append_event(conn, actor_id=actor_id, action="hub.resource.registered",
                             resource_type="hub_resource", resource_id=resource_id,
                             detail={"site_id": site_id, "kind": kind, "name": name},
                             occurred_at=self._now())
                return {"resource_type": "hub_resource", "resource_id": resource_id,
                        "resource": {"resource_id": resource_id, "site_id": site_id,
                                     "kind": kind, "name": name, "active": True}}

            return self._receipt(conn, request_id=request_id, action="hub.register_resource",
                                 payload=payload, create=create)

    def register_blockade(self, *, request_id: str, actor_id: str, blockade_id: str,
                          resource_id: str, start_ts: str, end_ts: str,
                          reason: str) -> dict[str, Any]:
        start = parse_minute(start_ts, "start_ts")
        end = parse_minute(end_ts, "end_ts")
        if end <= start:
            raise ValidationError("封锁结束时间必须晚于开始时间")
        payload = {"actor_id": actor_id, "blockade_id": blockade_id, "resource_id": resource_id,
                   "start_ts": format_ts(start), "end_ts": format_ts(end), "reason": reason}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            resource = conn.execute(
                "SELECT * FROM hub_resources WHERE resource_id=?", (resource_id,)
            ).fetchone()
            if resource is None:
                raise NotFoundError("作业资源不存在")
            reason = str(reason).strip() or "封锁窗口"

            def create():
                try:
                    conn.execute(
                        "INSERT INTO resource_blockades(blockade_id,resource_id,start_ts,end_ts,"
                        "reason,created_at) VALUES(?,?,?,?,?,?)",
                        (blockade_id, resource_id, format_ts(start), format_ts(end),
                         reason, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("封锁编号已经存在") from exc
                site_id = resource["site_id"]
                append_event(conn, actor_id=actor_id, action="hub.blockade.registered",
                             resource_type="blockade", resource_id=blockade_id,
                             detail={"resource_id": resource_id, "start_ts": format_ts(start),
                                     "end_ts": format_ts(end), "reason": reason},
                             occurred_at=self._now())
                # 封锁立即压垮与之重叠的未生效占用（只影响未来部分）。
                self._reconcile_site(conn, site_id)
                return {"resource_type": "blockade", "resource_id": blockade_id,
                        "blockade": {"blockade_id": blockade_id, "resource_id": resource_id,
                                     "start_ts": format_ts(start), "end_ts": format_ts(end),
                                     "reason": reason}}

            return self._receipt(conn, request_id=request_id, action="hub.register_blockade",
                                 payload=payload, create=create)

    def register_contract(self, *, request_id: str, actor_id: str, contract_id: str,
                          site_id: str, priority: int, name: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "contract_id": contract_id, "site_id": site_id,
                   "priority": priority, "name": name}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            self._site(conn, site_id)
            if not isinstance(priority, int) or priority < 0:
                raise ValidationError("priority 必须是非负整数")
            name = str(name).strip()
            if not name:
                raise ValidationError("name 不能为空")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO priority_contracts(contract_id,site_id,priority,name,created_at) "
                        "VALUES(?,?,?,?,?)",
                        (contract_id, site_id, priority, name, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("合同编号已经存在") from exc
                append_event(conn, actor_id=actor_id, action="hub.contract.registered",
                             resource_type="priority_contract", resource_id=contract_id,
                             detail={"site_id": site_id, "priority": priority, "name": name},
                             occurred_at=self._now())
                return {"resource_type": "priority_contract", "resource_id": contract_id,
                        "contract": {"contract_id": contract_id, "site_id": site_id,
                                     "priority": priority, "name": name}}

            return self._receipt(conn, request_id=request_id, action="hub.register_contract",
                                 payload=payload, create=create)

    def register_shipment(self, *, request_id: str, actor_id: str, shipment_id: str,
                          site_id: str, inbound_party: str, outbound_party: str,
                          groups: list[dict[str, Any]], contract_id: str | None = None) -> dict[str, Any]:
        if not isinstance(groups, list) or not groups:
            raise ValidationError("groups 必须是非空数组")
        clean_groups: list[dict[str, int | str]] = []
        for index, group in enumerate(groups):
            gid = str(group.get("group_id", "")).strip()
            qty = group.get("quantity")
            minutes = group.get("work_minutes")
            if not gid:
                raise ValidationError(f"groups[{index}].group_id 不能为空")
            if not isinstance(qty, int) or qty <= 0:
                raise ValidationError(f"groups[{index}].quantity 必须是正整数")
            if not isinstance(minutes, int) or minutes <= 0:
                raise ValidationError(f"groups[{index}].work_minutes 必须是正整数")
            clean_groups.append({"group_id": gid, "quantity": qty, "work_minutes": minutes})
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "site_id": site_id,
                   "inbound_party": inbound_party, "outbound_party": outbound_party,
                   "contract_id": contract_id, "groups": clean_groups}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            self._site(conn, site_id)
            if not self._org_exists(conn, inbound_party):
                raise NotFoundError("进站承运方不存在")
            if not self._org_exists(conn, outbound_party):
                raise NotFoundError("出站承运方不存在")
            if inbound_party == outbound_party:
                raise ValidationError("进站与出站承运方不能相同")
            if contract_id is not None and conn.execute(
                "SELECT 1 FROM priority_contracts WHERE contract_id=? AND site_id=?",
                (contract_id, site_id),
            ).fetchone() is None:
                raise NotFoundError("优先合同不存在")
            known: set[str] = set()
            for group in clean_groups:
                if group["group_id"] in known:
                    raise ValidationError("箱组编号在同一票内重复")
                known.add(group["group_id"])

            def create():
                try:
                    conn.execute(
                        "INSERT INTO shipments(shipment_id,site_id,contract_id,inbound_party,"
                        "outbound_party,created_at) VALUES(?,?,?,?,?,?)",
                        (shipment_id, site_id, contract_id, inbound_party, outbound_party,
                         self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("货票编号已经存在") from exc
                for position, group in enumerate(clean_groups):
                    conn.execute(
                        "INSERT INTO shipment_groups(group_id,shipment_id,position,quantity,"
                        "work_minutes,created_at) VALUES(?,?,?,?,?,?)",
                        (group["group_id"], shipment_id, position, group["quantity"],
                         group["work_minutes"], self._now()),
                    )
                append_event(conn, actor_id=actor_id, action="hub.shipment.registered",
                             resource_type="shipment", resource_id=shipment_id,
                             detail={"site_id": site_id, "inbound_party": inbound_party,
                                     "outbound_party": outbound_party,
                                     "contract_id": contract_id,
                                     "groups": clean_groups},
                             occurred_at=self._now())
                return {"resource_type": "shipment", "resource_id": shipment_id,
                        "shipment": {"shipment_id": shipment_id, "site_id": site_id,
                                     "inbound_party": inbound_party,
                                     "outbound_party": outbound_party,
                                     "contract_id": contract_id, "groups": clean_groups}}

            return self._receipt(conn, request_id=request_id, action="hub.register_shipment",
                                 payload=payload, create=create)

    def register_batch(self, *, request_id: str, actor_id: str, batch_id: str, site_id: str,
                       mode: str, direction: str, planned_arrival: str,
                       planned_departure: str | None = None,
                       manifest: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        if mode not in MODES:
            raise ValidationError("mode 必须是 rail/water/road")
        if direction not in DIRECTIONS:
            raise ValidationError("direction 必须是 inbound/outbound")
        arrival = parse_minute(planned_arrival, "planned_arrival")
        departure = parse_minute(planned_departure, "planned_departure") if planned_departure else None
        if departure is not None and departure <= arrival:
            raise ValidationError("计划出发必须晚于计划到达")
        manifest = manifest or []
        clean_manifest: list[dict[str, Any]] = []
        for index, item in enumerate(manifest):
            gid = str(item.get("group_id", "")).strip()
            qty = item.get("quantity")
            if not gid:
                raise ValidationError(f"manifest[{index}].group_id 不能为空")
            if not isinstance(qty, int) or qty <= 0:
                raise ValidationError(f"manifest[{index}].quantity 必须是正整数")
            clean_manifest.append({"group_id": gid, "quantity": qty})
        payload = {"actor_id": actor_id, "batch_id": batch_id, "site_id": site_id, "mode": mode,
                   "direction": direction, "planned_arrival": format_ts(arrival),
                   "planned_departure": format_ts(departure) if departure else None,
                   "manifest": clean_manifest}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            self._site(conn, site_id)
            shipment_by_group: dict[str, str] = {}
            for row in conn.execute(
                "SELECT g.group_id, g.shipment_id, g.quantity, s.site_id FROM shipment_groups g "
                "JOIN shipments s ON s.shipment_id=g.shipment_id WHERE s.site_id=?", (site_id,)
            ):
                shipment_by_group[row["group_id"]] = row["shipment_id"]
            for item in clean_manifest:
                if item["group_id"] not in shipment_by_group:
                    raise NotFoundError(f"清单箱组 {item['group_id']} 不属于本站任何货票")

            def create():
                try:
                    conn.execute(
                        "INSERT INTO batches(batch_id,site_id,mode,direction,planned_arrival,"
                        "planned_departure,created_at) VALUES(?,?,?,?,?,?,?)",
                        (batch_id, site_id, mode, direction, format_ts(arrival),
                         format_ts(departure) if departure else None, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("批次编号已经存在") from exc
                for position, item in enumerate(clean_manifest):
                    conn.execute(
                        "INSERT INTO batch_manifests(batch_id,group_id,quantity,position) "
                        "VALUES(?,?,?,?)",
                        (batch_id, item["group_id"], item["quantity"], position),
                    )
                append_event(conn, actor_id=actor_id, action="hub.batch.registered",
                             resource_type="batch", resource_id=batch_id,
                             detail={"site_id": site_id, "mode": mode, "direction": direction,
                                     "planned_arrival": format_ts(arrival),
                                     "planned_departure": format_ts(departure) if departure else None,
                                     "manifest": clean_manifest},
                             occurred_at=self._now())
                self._reconcile_site(conn, site_id)
                return {"resource_type": "batch", "resource_id": batch_id,
                        "batch": {"batch_id": batch_id, "site_id": site_id, "mode": mode,
                                  "direction": direction,
                                  "planned_arrival": format_ts(arrival),
                                  "planned_departure": format_ts(departure) if departure else None,
                                  "manifest": clean_manifest}}

            return self._receipt(conn, request_id=request_id, action="hub.register_batch",
                                 payload=payload, create=create)

    # ------------------------------------------------------------------ 事件入口

    def ingest_event(self, *, request_id: str, actor_id: str, event_id: str,
                     event_type: str, occurred_at: str, payload: dict[str, Any]) -> dict[str, Any]:
        """追加一条现场事件。乱序、重复投递都是安全的。"""

        if event_type not in EVENT_TYPES:
            raise ValidationError(f"未知事件类型 {event_type}")
        if not isinstance(payload, dict):
            raise ValidationError("payload 必须是对象")
        occurred = parse_ts(occurred_at, "occurred_at")
        clean_payload = self._validate_event_payload(event_type, occurred, payload)
        stored_payload = canonical_json(clean_payload)
        payload_hash = digest({"event_type": event_type, "occurred_at": format_ts(occurred),
                               "payload": clean_payload})
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")

            def create():
                # event_id 是自然幂等键：同一编号同内容即重放，不同内容即冲突。
                existing = conn.execute(
                    "SELECT * FROM hub_events WHERE event_id=?", (event_id,)
                ).fetchone()
                if existing is not None:
                    if existing["payload_hash"] != payload_hash:
                        raise ConflictError("event_id 已被不同内容使用")
                    return {"resource_type": "hub_event", "resource_id": event_id, "replayed": True,
                            "event": {"event_id": event_id, "event_type": event_type,
                                      "occurred_at": existing["occurred_at"],
                                      "payload": json.loads(existing["payload_json"])}
                            }
                self._validate_event_references(conn, event_type, clean_payload)
                conn.execute(
                    "INSERT INTO hub_events(event_id,site_id,event_type,occurred_at,payload_json,"
                    "payload_hash,actor_id,recorded_at) VALUES(?,?,?,?,?,?,?,?)",
                    (event_id, clean_payload["site_id"], event_type, format_ts(occurred),
                     stored_payload, payload_hash, actor_id, self._now()),
                )
                append_event(conn, actor_id=actor_id, action=f"hub.event.{event_type}",
                             resource_type="hub_event", resource_id=event_id,
                             detail={"occurred_at": format_ts(occurred),
                                     "payload": clean_payload},
                             occurred_at=self._now())
                summary = self._reconcile_site(conn, clean_payload["site_id"])
                return {"resource_type": "hub_event", "resource_id": event_id,
                        "event": {"event_id": event_id, "event_type": event_type,
                                  "occurred_at": format_ts(occurred),
                                  "payload": clean_payload},
                        "affected_task_ids": summary["affected_tasks"]}

            return self._receipt(conn, request_id=request_id,
                                 action=f"hub.ingest_event:{event_id}",
                                 payload={"event_id": event_id, "event_type": event_type,
                                          "occurred_at": format_ts(occurred),
                                          "payload": clean_payload},
                                 create=create)

    def _validate_event_payload(self, event_type: str, occurred, payload: dict[str, Any]) -> dict[str, Any]:
        site_id = str(payload.get("site_id", "")).strip()
        if not site_id:
            raise ValidationError("payload.site_id 不能为空")
        result: dict[str, Any] = {"site_id": site_id}
        if event_type in ("arrival", "arrival_qty", "drop_qty", "batch_departure"):
            batch_id = str(payload.get("batch_id", "")).strip()
            if not batch_id:
                raise ValidationError("payload.batch_id 不能为空")
            result["batch_id"] = batch_id
        if event_type in ("arrival_qty", "drop_qty"):
            group_id = str(payload.get("group_id", "")).strip()
            quantity = payload.get("quantity")
            if not group_id:
                raise ValidationError("payload.group_id 不能为空")
            if not isinstance(quantity, int) or quantity < 0:
                raise ValidationError("payload.quantity 必须是非负累计整数")
            result["group_id"] = group_id
            result["quantity"] = quantity
        if event_type == "arrival":
            actual = payload.get("actual_arrival")
            result["actual_arrival"] = format_ts(parse_ts(actual, "actual_arrival")) if actual \
                else format_ts(occurred)
        if event_type == "batch_departure":
            actual = payload.get("actual_departure")
            result["actual_departure"] = format_ts(parse_ts(actual, "actual_departure")) if actual \
                else format_ts(occurred)
        if event_type == "equipment_fault":
            resource_id = str(payload.get("resource_id", "")).strip()
            if not resource_id:
                raise ValidationError("payload.resource_id 不能为空")
            result["resource_id"] = resource_id
            start = parse_ts(payload.get("start_ts") or format_ts(occurred), "start_ts")
            result["start_ts"] = format_ts(start)
            if payload.get("end_ts"):
                result["end_ts"] = format_ts(parse_ts(payload["end_ts"], "end_ts"))
            result["reason"] = str(payload.get("reason", "设备故障")).strip() or "设备故障"
        if event_type == "equipment_recovered":
            resource_id = str(payload.get("resource_id", "")).strip()
            fault_event_id = str(payload.get("fault_event_id", "")).strip()
            if not resource_id or not fault_event_id:
                raise ValidationError("恢复事件必须携带 resource_id 与 fault_event_id")
            end = parse_ts(payload.get("end_ts") or format_ts(occurred), "end_ts")
            result["resource_id"] = resource_id
            result["fault_event_id"] = fault_event_id
            result["end_ts"] = format_ts(end)
        if event_type == "handover":
            shipment_id = str(payload.get("shipment_id", "")).strip()
            group_id = str(payload.get("group_id", "")).strip()
            stage = payload.get("stage")
            quantity = payload.get("quantity")
            if not shipment_id or not group_id:
                raise ValidationError("交接事件必须携带 shipment_id 与 group_id")
            if stage not in ("inbound", "outbound"):
                raise ValidationError("stage 必须是 inbound 或 outbound")
            if not isinstance(quantity, int) or quantity <= 0:
                raise ValidationError("quantity 必须是正整数")
            result.update({"shipment_id": shipment_id, "group_id": group_id, "stage": stage,
                           "quantity": quantity,
                           "at_ts": format_ts(parse_ts(payload["at_ts"], "at_ts"))
                           if payload.get("at_ts") else format_ts(occurred)})
        return result

    def _validate_event_references(self, conn, event_type: str, payload: dict[str, Any]) -> None:
        """在写入事件前校验静态引用；动态配对允许乱序（如恢复先于故障）。"""
        site_id = payload["site_id"]
        if conn.execute("SELECT 1 FROM sites WHERE site_id=?", (site_id,)).fetchone() is None:
            raise NotFoundError("场所不存在")
        if event_type in ("arrival", "batch_departure"):
            if conn.execute(
                    "SELECT 1 FROM batches WHERE batch_id=? AND site_id=?",
                    (payload["batch_id"], site_id)).fetchone() is None:
                raise NotFoundError("事件引用的批次不存在或不属于该场所")
        if event_type in ("arrival_qty", "drop_qty"):
            row = conn.execute(
                "SELECT 1 FROM batch_manifests m JOIN batches b ON b.batch_id=m.batch_id "
                "WHERE m.batch_id=? AND m.group_id=? AND b.site_id=?",
                (payload["batch_id"], payload["group_id"], site_id)).fetchone()
            if row is None:
                raise NotFoundError("箱组不在该批次清单中")
        if event_type == "equipment_fault":
            if conn.execute(
                    "SELECT 1 FROM hub_resources WHERE resource_id=? AND site_id=?",
                    (payload["resource_id"], site_id)).fetchone() is None:
                raise NotFoundError("事件引用的作业资源不存在")
        if event_type == "equipment_recovered":
            if conn.execute(
                    "SELECT 1 FROM hub_resources WHERE resource_id=? AND site_id=?",
                    (payload["resource_id"], site_id)).fetchone() is None:
                raise NotFoundError("事件引用的作业资源不存在")
            # 故障消息尚未到达是合法乱序，不做配对校验。
        if event_type == "handover":
            row = conn.execute(
                "SELECT 1 FROM shipment_groups g JOIN shipments s ON s.shipment_id=g.shipment_id "
                "WHERE g.group_id=? AND g.shipment_id=? AND s.site_id=?",
                (payload["group_id"], payload["shipment_id"], site_id)).fetchone()
            if row is None:
                raise NotFoundError("交接事件引用的货票/箱组不存在")

    # ------------------------------------------------------------------ 确定性折叠

    def _fold(self, conn, site_id: str) -> dict[str, Any]:
        """把全部只追加事件按规范顺序折叠为当前事实（纯函数式读取）。"""
        batches: dict[str, dict[str, Any]] = {}
        for row in conn.execute("SELECT * FROM batches WHERE site_id=? ORDER BY batch_id", (site_id,)):
            batches[row["batch_id"]] = {
                "batch_id": row["batch_id"], "mode": row["mode"], "direction": row["direction"],
                "planned_arrival": row["planned_arrival"],
                "planned_departure": row["planned_departure"],
                "actual_arrival": None, "actual_departure": None, "closed": False,
                "manifest": {},
            }
        for row in conn.execute(
            "SELECT m.batch_id,m.group_id,m.quantity FROM batch_manifests m JOIN batches b "
            "ON b.batch_id=m.batch_id WHERE b.site_id=? ORDER BY m.position", (site_id,)
        ):
            batches[row["batch_id"]]["manifest"][row["group_id"]] = row["quantity"]

        # 到达量、甩箱量、实际到发时刻全部只从事件折叠，不依赖任何可变派生表。
        arrived: dict[tuple[str, str], int] = {}
        dropped: dict[tuple[str, str], int] = {}

        outages: dict[str, dict[str, Any]] = {}
        handovers: list[dict[str, Any]] = []
        # 规范顺序：业务发生时间优先，事件编号兜底，杜绝投递顺序影响结果。
        for row in conn.execute(
            "SELECT * FROM hub_events WHERE site_id=? ORDER BY occurred_at, event_id", (site_id,)
        ):
            payload = json.loads(row["payload_json"])
            if row["event_type"] == "arrival":
                bid = payload["batch_id"]
                if bid in batches:
                    batches[bid]["actual_arrival"] = payload["actual_arrival"]
            elif row["event_type"] == "batch_departure":
                bid = payload["batch_id"]
                if bid in batches:
                    batches[bid]["actual_departure"] = payload["actual_departure"]
                    batches[bid]["closed"] = True
            elif row["event_type"] == "arrival_qty":
                key = (payload["batch_id"], payload["group_id"])
                # 累计最大值：迟到的更大值生效，重放同值无变化，任何路径都不会减少。
                arrived[key] = max(arrived.get(key, 0), payload["quantity"])
            elif row["event_type"] == "drop_qty":
                key = (payload["batch_id"], payload["group_id"])
                dropped[key] = max(dropped.get(key, 0), payload["quantity"])
            elif row["event_type"] == "equipment_fault":
                # 恢复消息可能先到（占位），故障消息后到时保留已闭合的 end_ts。
                prior = outages.get(row["event_id"])
                outages[row["event_id"]] = {
                    "event_id": row["event_id"], "resource_id": payload["resource_id"],
                    "start_ts": payload["start_ts"],
                    "end_ts": payload.get("end_ts") or (prior["end_ts"] if prior else None),
                    "reason": payload.get("reason", "设备故障"),
                }
            elif row["event_type"] == "equipment_recovered":
                outage = outages.get(payload["fault_event_id"])
                end_ts = payload["end_ts"]
                if outage is None:
                    # 恢复消息先于故障消息到达：建占位，故障消息随后归并。
                    outages[payload["fault_event_id"]] = {
                        "event_id": payload["fault_event_id"],
                        "resource_id": payload["resource_id"], "start_ts": None,
                        "end_ts": end_ts, "reason": "设备故障"}
                elif outage["resource_id"] == payload["resource_id"] and (
                        outage["end_ts"] is None or end_ts < outage["end_ts"]):
                    outage["end_ts"] = end_ts
            elif row["event_type"] == "handover":
                handovers.append({"event_id": row["event_id"], **payload})
        handovers.sort(key=lambda h: (h["at_ts"], h["event_id"], h["stage"]))

        groups: dict[str, dict[str, Any]] = {}
        for row in conn.execute(
            "SELECT g.*, s.site_id AS s_site, s.inbound_party, s.outbound_party, s.contract_id "
            "FROM shipment_groups g JOIN shipments s ON s.shipment_id=g.shipment_id WHERE s.site_id=?",
            (site_id,),
        ):
            groups[row["group_id"]] = {
                "group_id": row["group_id"], "shipment_id": row["shipment_id"],
                "position": row["position"], "quantity": row["quantity"],
                "work_minutes": row["work_minutes"],
                "inbound_party": row["inbound_party"], "outbound_party": row["outbound_party"],
                "contract_id": row["contract_id"]}
        inbound_batches: dict[str, list[str]] = {gid: [] for gid in groups}
        outbound_batches: dict[str, list[str]] = {gid: [] for gid in groups}
        for bid, batch in batches.items():
            target = inbound_batches if batch["direction"] == "inbound" else outbound_batches
            for gid in batch["manifest"]:
                if gid in target:
                    target[gid].append(bid)
        for listings in (inbound_batches, outbound_batches):
            for gid in listings:
                listings[gid].sort()

        return {"batches": batches, "arrived": arrived, "dropped": dropped,
                "outages": list(outages.values()), "handovers": handovers,
                "groups": groups, "inbound_batches": inbound_batches,
                "outbound_batches": outbound_batches}

    def _group_facts(self, state: dict[str, Any], gid: str) -> dict[str, Any]:
        """由折叠事实计算单个箱组的派生量（全部为单调量）。"""
        group = state["groups"][gid]
        planned = group["quantity"]
        arrived_total = 0
        dropped_total = 0
        raw_arrived_total = 0
        raw_dropped_total = 0
        inbound_ready = None
        planned_ready = None
        for bid in state["inbound_batches"].get(gid, []):
            batch = state["batches"][bid]
            manifest_qty = batch["manifest"].get(gid, planned)
            raw_arrived = state["arrived"].get((bid, gid), 0)
            raw_dropped = state["dropped"].get((bid, gid), 0)
            raw_arrived_total += raw_arrived
            raw_dropped_total += raw_dropped
            # 守恒钳制：到达量不超过清单；甩箱是到达箱的子集，不超过到达量。
            arrived_here = min(raw_arrived, manifest_qty)
            dropped_here = min(raw_dropped, arrived_here)
            arrived_total += arrived_here
            dropped_total += dropped_here
            planned_arrival = batch["planned_arrival"]
            if planned_arrival and (planned_ready is None or planned_arrival < planned_ready):
                planned_ready = planned_arrival
            if arrived_here > 0:
                ready_ts = batch["actual_arrival"] or planned_arrival
                if ready_ts and (inbound_ready is None or ready_ts < inbound_ready):
                    inbound_ready = ready_ts
        inbound_h = 0
        outbound_h = 0
        first_inbound_h = None
        last_outbound_h = None
        for handover in state["handovers"]:
            if handover["group_id"] != gid or handover["shipment_id"] != group["shipment_id"]:
                continue
            if handover["stage"] == "inbound":
                inbound_h += handover["quantity"]
                if first_inbound_h is None or handover["at_ts"] < first_inbound_h:
                    first_inbound_h = handover["at_ts"]
            else:
                outbound_h += handover["quantity"]
                if last_outbound_h is None or handover["at_ts"] > last_outbound_h:
                    last_outbound_h = handover["at_ts"]
        completed = outbound_h
        dropped_total = min(dropped_total, max(planned - completed, 0))
        remaining = max(planned - completed - dropped_total, 0)
        actual_ready = first_inbound_h or inbound_ready
        # 无实际消息时退回静态时刻，方案不会排到计划到达之前；延误后以实际时刻为准。
        ready_at = actual_ready or planned_ready
        due_at = None
        for bid in state["outbound_batches"].get(gid, []):
            departure = state["batches"][bid]["planned_departure"]
            if departure and (due_at is None or departure < due_at):
                due_at = departure
        violations = []
        if raw_arrived_total > planned:
            violations.append("arrived_exceeds_manifest")
        if raw_dropped_total > raw_arrived_total:
            violations.append("dropped_exceeds_arrival")
        if inbound_h > arrived_total:
            violations.append("inbound_handover_exceeds_arrival")
        if outbound_h > inbound_h:
            violations.append("outbound_handover_exceeds_inbound")
        if dropped_total + completed > planned:
            violations.append("drop_and_complete_exceeds_manifest")
        return {"group": group, "planned": planned, "arrived": arrived_total,
                "dropped": dropped_total, "inbound_handed": inbound_h,
                "outbound_handed": outbound_h, "completed": completed,
                "remaining": remaining, "ready_at": ready_at,
                "actual_ready_at": actual_ready, "planned_ready_at": planned_ready,
                "due_at": due_at,
                "first_inbound_handover": first_inbound_h,
                "last_outbound_handover": last_outbound_h, "violations": violations}

    def _reconcile_site(self, conn, site_id: str) -> dict[str, Any]:
        """根据折叠结果重算派生任务并释放失效占用；对消息顺序与重启幂等。"""
        state = self._fold(conn, site_id)
        now = self._now()
        affected: list[str] = []

        # 超过确认期限仍开口的方案自动失效，任务回退等待新方案。
        for expired in conn.execute(
                "SELECT plan_id FROM plans WHERE site_id=? AND status='open' AND expires_at<=?",
                (site_id, now)).fetchall():
            conn.execute("UPDATE plans SET status='expired' WHERE plan_id=?",
                         (expired["plan_id"],))
            conn.execute(
                "UPDATE transfer_tasks SET status='pending',plan_id=NULL,updated_at=? "
                "WHERE plan_id=? AND status='planned'", (now, expired["plan_id"]))
            append_event(conn, actor_id="system", action="hub.plan.expired",
                         resource_type="plan", resource_id=expired["plan_id"],
                         detail={"expires_at": now}, occurred_at=now)

        resources: dict[str, dict[str, Any]] = {}
        for row in conn.execute("SELECT * FROM hub_resources WHERE site_id=?", (site_id,)):
            resources[row["resource_id"]] = {"kind": row["kind"], "active": bool(row["active"])}
        blockades: dict[str, list[tuple[str, str]]] = {}
        for row in conn.execute(
            "SELECT b.* FROM resource_blockades b JOIN hub_resources r ON r.resource_id=b.resource_id "
            "WHERE r.site_id=? ORDER BY b.start_ts", (site_id,)
        ):
            blockades.setdefault(row["resource_id"], []).append((row["start_ts"], row["end_ts"]))
        outages_by_resource: dict[str, list[dict[str, Any]]] = {}
        for outage in state["outages"]:
            outages_by_resource.setdefault(outage["resource_id"], []).append(outage)

        contract_priority: dict[str | None, int] = {}
        for row in conn.execute("SELECT contract_id,priority FROM priority_contracts WHERE site_id=?",
                                (site_id,)):
            contract_priority[row["contract_id"]] = row["priority"]

        facts_by_group = {gid: self._group_facts(state, gid) for gid in state["groups"]}
        for gid, facts in facts_by_group.items():
            task_id = f"task-{gid}"
            existing = conn.execute("SELECT * FROM transfer_tasks WHERE task_id=?", (task_id,)).fetchone()
            group = facts["group"]
            if facts["violations"]:
                base_status = "exception"
            elif facts["remaining"] == 0:
                base_status = "completed" if facts["completed"] >= facts["planned"] else "cancelled"
            elif facts["completed"] > 0:
                base_status = "in_progress"
            else:
                base_status = "pending"
            ready_value = facts["ready_at"] or now
            if existing is None:
                conn.execute(
                    "INSERT INTO transfer_tasks(task_id,group_id,site_id,shipment_id,planned_qty,"
                    "completed_qty,dropped_qty,remaining_qty,ready_at,due_at,work_minutes,status,"
                    "plan_id,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (task_id, gid, site_id, group["shipment_id"], facts["planned"],
                     facts["completed"], facts["dropped"], facts["remaining"],
                     ready_value, facts["due_at"], group["work_minutes"],
                     base_status, None, now))
            else:
                # planned/scheduled 归属由建案、确认、占用释放流程维护；
                # 除非折叠事实显示任务终结、异常或已有部分完成，否则保持现状。
                if existing["status"] in ("planned", "scheduled") and base_status not in (
                        "exception", "completed", "cancelled"):
                    if base_status == "in_progress":
                        final_status = "in_progress"
                    else:
                        final_status = existing["status"]
                else:
                    final_status = base_status
                conn.execute(
                    "UPDATE transfer_tasks SET planned_qty=?,completed_qty=?,dropped_qty=?,"
                    "remaining_qty=?,ready_at=?,due_at=?,work_minutes=?,status=?,"
                    "updated_at=? WHERE task_id=?",
                    (facts["planned"], facts["completed"], facts["dropped"], facts["remaining"],
                     ready_value, facts["due_at"], group["work_minutes"],
                     final_status, now, task_id))

        # 失效占用判定：先按任务识别命中，再释放该任务所有尚未结束的占用。
        # 历史占用（end_ts <= now）永不回退，保证交接流水与责任链稳定。
        held = conn.execute(
            "SELECT o.* FROM occupancies o WHERE o.site_id=? AND o.status='held' "
            "ORDER BY o.start_ts,o.occupancy_id", (site_id,)).fetchall()
        held_by_task: dict[str, list] = {}
        for occ in held:
            if occ["end_ts"] <= now:
                continue
            held_by_task.setdefault(occ["task_id"], []).append(occ)

        def occupancy_reason(occ) -> str | None:
            resource = resources.get(occ["resource_id"])
            if resource is None or not resource["active"]:
                return "resource_unavailable"
            for start_b, end_b in blockades.get(occ["resource_id"], ()):
                if overlaps(parse_ts(occ["start_ts"]), parse_ts(occ["end_ts"]),
                            parse_ts(start_b), parse_ts(end_b)):
                    return "blockade"
            for outage in outages_by_resource.get(occ["resource_id"], ()):
                if not outage["start_ts"]:
                    continue
                end_out = outage["end_ts"] or occ["end_ts"]
                if overlaps(parse_ts(occ["start_ts"]), parse_ts(occ["end_ts"]),
                            parse_ts(outage["start_ts"]), parse_ts(end_out)):
                    return "equipment_fault"
            facts = facts_by_group.get(occ["group_id"])
            if facts is not None:
                if facts["violations"]:
                    return "data_exception"
                if facts["remaining"] == 0 and occ["start_ts"] >= now:
                    return "completed_or_dropped"
                if facts["ready_at"] and parse_ts(occ["start_ts"]) < parse_ts(facts["ready_at"]):
                    return "arrival_delay"
            return None

        for task_id, occs in held_by_task.items():
            hit_reason = None
            for occ in occs:
                hit_reason = occupancy_reason(occ)
                if hit_reason:
                    break
            if hit_reason is None:
                continue
            task = conn.execute(
                "SELECT * FROM transfer_tasks WHERE task_id=?", (task_id,)).fetchone()
            facts = facts_by_group.get(occs[0]["group_id"])
            for occ in occs:
                conn.execute(
                    "UPDATE occupancies SET status='released',release_reason=?,released_at=? "
                    "WHERE occupancy_id=? AND status='held'", (hit_reason, now, occ["occupancy_id"]))
                append_event(conn, actor_id="system", action="hub.occupancy.released",
                             resource_type="occupancy", resource_id=occ["occupancy_id"],
                             detail={"plan_id": occ["plan_id"], "task_id": occ["task_id"],
                                     "group_id": occ["group_id"], "resource_id": occ["resource_id"],
                                     "start_ts": occ["start_ts"], "end_ts": occ["end_ts"],
                                     "reason": hit_reason}, occurred_at=now)
            if task is not None and task["status"] in ("scheduled", "planned"):
                next_status = "in_progress" if facts and facts["completed"] > 0 \
                    and facts["remaining"] > 0 else "pending"
                conn.execute(
                    "UPDATE transfer_tasks SET status=?,plan_id=NULL,updated_at=? WHERE task_id=?",
                    (next_status, now, task_id))
            if occs[0]["group_id"] not in affected:
                affected.append(occs[0]["group_id"])

        return {"affected_tasks": sorted(f"task-{gid}" for gid in affected)}

    # ------------------------------------------------------------------ 两阶段计划

    def _busy_intervals(self, conn, site_id: str, resource_id: str,
                        state: dict[str, Any]) -> list[tuple]:
        intervals: list[tuple] = []
        for row in conn.execute(
            "SELECT b.start_ts AS s,b.end_ts AS e FROM resource_blockades b WHERE b.resource_id=?",
            (resource_id,),
        ):
            intervals.append((parse_ts(row["s"]), parse_ts(row["e"]), "blockade"))
        for outage in state["outages"]:
            if outage["resource_id"] != resource_id or not outage["start_ts"]:
                continue
            end = parse_ts(outage["end_ts"]) if outage["end_ts"] else None
            intervals.append((parse_ts(outage["start_ts"]), end, "equipment_fault"))
        for row in conn.execute(
            "SELECT o.start_ts AS s,o.end_ts AS e FROM occupancies o WHERE o.resource_id=? "
            "AND o.status='held'", (resource_id,),
        ):
            intervals.append((parse_ts(row["s"]), parse_ts(row["e"]), "held"))
        intervals.sort(key=lambda item: item[0])
        return intervals

    def _earniest_slot(self, intervals, duration, earliest, horizon_end):
        """在忙区间列表中找出长度为 duration 的最早空档；故障未恢复视为持续到窗口外。"""
        candidate = earliest
        far_future = add_minutes(horizon_end, 1)
        while True:
            moved = False
            for start, end, _kind in intervals:
                real_end = end or far_future
                if add_minutes(candidate, duration) <= start:
                    continue
                if candidate < real_end:
                    candidate = real_end
                    moved = True
            if not moved:
                break
        if add_minutes(candidate, duration) > horizon_end:
            return None
        return candidate

    def create_plan(self, *, request_id: str, actor_id: str, site_id: str,
                    horizon_start: str, horizon_end: str, ttl_minutes: int = 30) -> dict[str, Any]:
        horizon_start_dt = parse_minute(horizon_start, "horizon_start")
        horizon_end_dt = parse_minute(horizon_end, "horizon_end")
        if horizon_end_dt <= horizon_start_dt:
            raise ValidationError("计划窗口结束必须晚于开始")
        if not isinstance(ttl_minutes, int) or ttl_minutes <= 0:
            raise ValidationError("ttl_minutes 必须是正整数")
        payload = {"actor_id": actor_id, "site_id": site_id,
                   "horizon_start": format_ts(horizon_start_dt),
                   "horizon_end": format_ts(horizon_end_dt), "ttl_minutes": ttl_minutes}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            self._site(conn, site_id)

            def create():
                self._reconcile_site(conn, site_id)
                state = self._fold(conn, site_id)
                plan_id = f"plan-{uuid.uuid4().hex[:16]}"
                generation_row = conn.execute(
                    "SELECT COALESCE(MAX(generation),0)+1 AS g FROM plans WHERE site_id=?",
                    (site_id,)).fetchone()
                generation = generation_row["g"]
                # 同一站点只保留一个在途方案：旧方案自动让位。
                conn.execute(
                    "UPDATE plans SET status='superseded' WHERE site_id=? AND status='open'",
                    (site_id,))
                conn.execute(
                    "UPDATE transfer_tasks SET status='pending',plan_id=NULL,updated_at=? "
                    "WHERE site_id=? AND status='planned'", (self._now(), site_id))

                cranes = [r["resource_id"] for r in conn.execute(
                    "SELECT resource_id FROM hub_resources WHERE site_id=? AND kind='crane' "
                    "AND active=1 ORDER BY resource_id", (site_id,))]
                slots = [r["resource_id"] for r in conn.execute(
                    "SELECT resource_id FROM hub_resources WHERE site_id=? AND kind='slot' "
                    "AND active=1 ORDER BY resource_id", (site_id,))]
                busy = {rid: self._busy_intervals(conn, site_id, rid, state)
                        for rid in cranes + slots}
                # 仍持有未结束占用的任务（scheduled 或部分完成后 in_progress）不重复排程。
                held_task_ids = {r["task_id"] for r in conn.execute(
                    "SELECT DISTINCT task_id FROM occupancies WHERE site_id=? AND status='held' "
                    "AND end_ts>?", (site_id, self._now())).fetchall()}

                candidates = []
                unscheduled = []
                tasks_rows = conn.execute(
                    "SELECT * FROM transfer_tasks WHERE site_id=? AND remaining_qty>0 "
                    "AND status NOT IN ('exception','scheduled','planned')", (site_id,)).fetchall()
                tasks_rows = [r for r in tasks_rows if r["task_id"] not in held_task_ids]

                def sort_key(row):
                    facts = self._group_facts(state, row["group_id"])
                    priority = 10 ** 9
                    if facts["group"]["contract_id"]:
                        crow = conn.execute(
                            "SELECT priority FROM priority_contracts WHERE contract_id=?",
                            (facts["group"]["contract_id"],)).fetchone()
                        if crow:
                            priority = crow["priority"]
                    ready_dt = parse_ts(facts["ready_at"]) if facts["ready_at"] else horizon_start_dt
                    return (priority, ready_dt, row["shipment_id"], row["task_id"])

                for task_row in sorted(tasks_rows, key=sort_key):
                    gid = task_row["group_id"]
                    facts = self._group_facts(state, gid)
                    duration = task_row["work_minutes"]
                    earliest = horizon_start_dt
                    if facts["ready_at"]:
                        ready_dt = parse_ts(facts["ready_at"])
                        if ready_dt > earliest:
                            earliest = ready_dt
                    best = None
                    reason = None
                    if not cranes or not slots:
                        reason = "no_available_resource"
                    else:
                        for crane in cranes:
                            for slot in slots:
                                crane_start = self._earniest_slot(
                                    busy[crane], duration, earliest, horizon_end_dt)
                                if crane_start is None:
                                    reason = "no_capacity_in_horizon"
                                    continue
                                slot_earliest = add_minutes(crane_start, duration)
                                slot_start = self._earniest_slot(
                                    busy[slot], duration, slot_earliest, horizon_end_dt)
                                if slot_start is None:
                                    reason = "no_capacity_in_horizon"
                                    continue
                                end = add_minutes(slot_start, duration)
                                choice = (end, crane_start, crane, slot, slot_start)
                                if best is None or choice < best:
                                    best = choice
                    if best is None:
                        if reason is None:
                            reason = "no_capacity_in_horizon"
                        unscheduled.append({"task_id": task_row["task_id"], "group_id": gid,
                                            "reason": self._explain_reason(state, task_row, reason)})
                        continue
                    end, crane_start, crane, slot, slot_start = best
                    intervals = [
                        (crane, crane_start, add_minutes(crane_start, duration)),
                        (slot, slot_start, add_minutes(slot_start, duration)),
                    ]
                    for rid, start_i, end_i in intervals:
                        busy[rid].append((start_i, end_i, "proposed"))
                        busy[rid].sort(key=lambda item: item[0])
                    candidates.append({"task_id": task_row["task_id"], "group_id": gid,
                                       "shipment_id": task_row["shipment_id"],
                                       "crane": crane, "slot": slot,
                                       "crane_start": format_ts(crane_start),
                                       "crane_end": format_ts(add_minutes(crane_start, duration)),
                                       "slot_start": format_ts(slot_start),
                                       "slot_end": format_ts(end),
                                       "work_minutes": duration})

                required_parties = sorted({
                    state["groups"][item["group_id"]]["inbound_party"] for item in candidates
                } | {
                    state["groups"][item["group_id"]]["outbound_party"] for item in candidates
                })
                expires_at = add_minutes(self._now_dt(), ttl_minutes)
                allocation_hash = digest(candidates)
                conn.execute(
                    "INSERT INTO plans(plan_id,site_id,generation,status,allocation_json,"
                    "allocation_hash,required_parties_json,reject_reason,created_at,expires_at,"
                    "confirmed_at) VALUES(?,?,?,'open',?,?,?,NULL,?,?,NULL)",
                    (plan_id, site_id, generation, canonical_json(candidates), allocation_hash,
                     canonical_json(required_parties), self._now(), format_ts(expires_at)))
                for item in candidates:
                    conn.execute(
                        "UPDATE transfer_tasks SET status='planned',plan_id=?,updated_at=? "
                        "WHERE task_id=?", (plan_id, self._now(), item["task_id"]))
                append_event(conn, actor_id=actor_id, action="hub.plan.proposed",
                             resource_type="plan", resource_id=plan_id,
                             detail={"generation": generation, "task_count": len(candidates),
                                     "unscheduled": unscheduled,
                                     "required_parties": required_parties,
                                     "expires_at": format_ts(expires_at)},
                             occurred_at=self._now())
                return {"resource_type": "plan", "resource_id": plan_id,
                        "plan": {"plan_id": plan_id, "site_id": site_id,
                                 "generation": generation, "status": "open",
                                 "expires_at": format_ts(expires_at),
                                 "required_parties": required_parties,
                                 "allocation": candidates, "unscheduled": unscheduled}}

            return self._receipt(conn, request_id=request_id, action="hub.create_plan",
                                 payload=payload, create=create)

    def _explain_reason(self, state, task_row, fallback: str) -> str:
        facts = self._group_facts(state, task_row["group_id"])
        if not facts["ready_at"]:
            return "awaiting_arrival"
        return fallback

    def confirm_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                     party: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "plan_id": plan_id, "party": party}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            plan = conn.execute("SELECT * FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
            if plan is None:
                raise NotFoundError("方案不存在")
            required = json.loads(plan["required_parties_json"])
            if actor.role != "admin" and actor.organization_id != party:
                raise PermissionDenied("只能由对应承运方组织或管理员确认")
            if party not in required:
                raise ValidationError("该方不在方案确认名单内")

            def create():
                if plan["status"] != "open":
                    raise ConflictError(f"方案状态为 {plan['status']}，不能确认")
                if parse_ts(plan["expires_at"]) <= self._now_dt():
                    conn.execute("UPDATE plans SET status='expired' WHERE plan_id=?", (plan_id,))
                    conn.execute(
                        "UPDATE transfer_tasks SET status='pending',plan_id=NULL,updated_at=? "
                        "WHERE plan_id=? AND status='planned'", (self._now(), plan_id))
                    append_event(conn, actor_id=actor_id, action="hub.plan.expired",
                                 resource_type="plan", resource_id=plan_id, detail={},
                                 occurred_at=self._now())
                    # 过期是持久事实：提交后把结果返回给调用方，而不是回滚。
                    return {"resource_type": "plan", "resource_id": plan_id,
                            "confirmation": {"plan_id": plan_id, "party": party,
                                             "status": "expired", "occupancies": [],
                                             "confirmed_parties": sorted({r["party"] for r in conn.execute(
                                                 "SELECT party FROM plan_confirmations WHERE plan_id=?",
                                                 (plan_id,))}),
                                             "required_parties": required}}
                already = conn.execute(
                    "SELECT 1 FROM plan_confirmations WHERE plan_id=? AND party=?",
                    (plan_id, party)).fetchone()
                if already is not None:
                    raise ConflictError("该方已经确认过")
                conn.execute(
                    "INSERT INTO plan_confirmations(plan_id,party,actor_id,confirmed_at) "
                    "VALUES(?,?,?,?)", (plan_id, party, actor_id, self._now()))
                append_event(conn, actor_id=actor_id, action="hub.plan.confirmed",
                             resource_type="plan", resource_id=plan_id,
                             detail={"party": party}, occurred_at=self._now())

                confirmed = {r["party"] for r in conn.execute(
                    "SELECT party FROM plan_confirmations WHERE plan_id=?", (plan_id,))}
                result = {"resource_type": "plan", "resource_id": plan_id,
                          "confirmation": {"plan_id": plan_id, "party": party,
                                           "confirmed_parties": sorted(confirmed),
                                           "required_parties": required,
                                           "occupancies": [], "status": "open"}}
                if confirmed != set(required):
                    return result

                # 全部确认：单事务重验全部区间并原子占用。
                state = self._fold(conn, plan["site_id"])
                allocation = json.loads(plan["allocation_json"])
                conflicts = []
                for item in allocation:
                    task_row = conn.execute(
                        "SELECT * FROM transfer_tasks WHERE task_id=?", (item["task_id"],)
                    ).fetchone()
                    facts = self._group_facts(state, item["group_id"])
                    # 任务级复验：方案开口期间可能发生甩箱、部分交接、延误或异常。
                    if task_row is None or task_row["plan_id"] != plan_id \
                            or task_row["status"] != "planned":
                        conflicts.append({"task_id": item["task_id"], "resource_id": None,
                                          "reason": "task_no_longer_open"})
                    elif facts["violations"]:
                        conflicts.append({"task_id": item["task_id"], "resource_id": None,
                                          "reason": "data_exception"})
                    elif facts["remaining"] <= 0:
                        conflicts.append({"task_id": item["task_id"], "resource_id": None,
                                          "reason": "nothing_remaining"})
                    elif facts["ready_at"] and parse_ts(item["crane_start"]) < parse_ts(facts["ready_at"]):
                        conflicts.append({"task_id": item["task_id"], "resource_id": None,
                                          "reason": "arrival_delay"})
                    duration = item["work_minutes"]
                    checks = [(item["crane"], item["crane_start"], item["crane_end"]),
                              (item["slot"], item["slot_start"], item["slot_end"])]
                    for rid, start_s, end_s in checks:
                        resource = conn.execute(
                            "SELECT * FROM hub_resources WHERE resource_id=? AND active=1",
                            (rid,)).fetchone()
                        if resource is None:
                            conflicts.append({"task_id": item["task_id"], "resource_id": rid,
                                              "reason": "resource_unavailable"})
                            continue
                        sdt, edt = parse_ts(start_s), parse_ts(end_s)
                        for row in conn.execute(
                            "SELECT start_ts,end_ts FROM resource_blockades WHERE resource_id=?",
                            (rid,)):
                            if overlaps(sdt, edt, parse_ts(row["start_ts"]), parse_ts(row["end_ts"])):
                                conflicts.append({"task_id": item["task_id"], "resource_id": rid,
                                                  "reason": "blockade"})
                        for outage in state["outages"]:
                            if outage["resource_id"] != rid or not outage["start_ts"]:
                                continue
                            o_end = parse_ts(outage["end_ts"]) if outage["end_ts"] else edt
                            if overlaps(sdt, edt, parse_ts(outage["start_ts"]), o_end):
                                conflicts.append({"task_id": item["task_id"], "resource_id": rid,
                                                  "reason": "equipment_fault"})
                        for row in conn.execute(
                            "SELECT start_ts,end_ts FROM occupancies WHERE resource_id=? "
                            "AND status='held'", (rid,)):
                            if overlaps(sdt, edt, parse_ts(row["start_ts"]), parse_ts(row["end_ts"])):
                                conflicts.append({"task_id": item["task_id"], "resource_id": rid,
                                                  "reason": "capacity_taken"})
                if conflicts:
                    conn.execute(
                        "UPDATE plans SET status='rejected',reject_reason=? WHERE plan_id=?",
                        (canonical_json(conflicts), plan_id))
                    conn.execute(
                        "UPDATE transfer_tasks SET status='pending',plan_id=NULL,updated_at=? "
                        "WHERE plan_id=?", (self._now(), plan_id))
                    append_event(conn, actor_id=actor_id, action="hub.plan.rejected",
                                 resource_type="plan", resource_id=plan_id,
                                 detail={"conflicts": conflicts}, occurred_at=self._now())
                    # 原子性保证：到此为止没有写入任何占用；拒绝结论本身提交。
                    result["confirmation"].update({"status": "rejected",
                                                   "conflicts": conflicts})
                    return result

                occupancies = []
                for item in allocation:
                    for rid, start_s, end_s, kind in (
                            (item["crane"], item["crane_start"], item["crane_end"], "crane"),
                            (item["slot"], item["slot_start"], item["slot_end"], "slot")):
                        occ_id = "occ-" + digest([plan_id, item["task_id"], rid, start_s])[:24]
                        conn.execute(
                            "INSERT INTO occupancies(occupancy_id,site_id,plan_id,task_id,group_id,"
                            "resource_id,start_ts,end_ts,status,created_at) "
                            "VALUES(?,?,?,?,?,?,?,?,'held',?)",
                            (occ_id, plan["site_id"], plan_id, item["task_id"], item["group_id"],
                             rid, start_s, end_s, self._now()))
                        occupancies.append({"occupancy_id": occ_id, "resource_id": rid,
                                            "kind": kind, "start_ts": start_s, "end_ts": end_s,
                                            "task_id": item["task_id"]})
                conn.execute(
                    "UPDATE plans SET status='confirmed',confirmed_at=? WHERE plan_id=?",
                    (self._now(), plan_id))
                conn.execute(
                    "UPDATE transfer_tasks SET status='scheduled',updated_at=? WHERE plan_id=?",
                    (self._now(), plan_id))
                append_event(conn, actor_id=actor_id, action="hub.plan.committed",
                             resource_type="plan", resource_id=plan_id,
                             detail={"occupancy_count": len(occupancies)},
                             occurred_at=self._now())
                result["confirmation"].update({"status": "confirmed", "occupancies": occupancies})
                return result

            return self._receipt(conn, request_id=request_id,
                                 action=f"hub.confirm_plan:{plan_id}:{party}",
                                 payload=payload, create=create)

    # ------------------------------------------------------------------ 冻结考核

    def freeze_kpi(self, *, request_id: str, actor_id: str, site_id: str,
                   window_start: str, window_end: str) -> dict[str, Any]:
        start = parse_minute(window_start, "window_start")
        end = parse_minute(window_end, "window_end")
        if end <= start:
            raise ValidationError("考核窗口结束必须晚于开始")
        payload = {"actor_id": actor_id, "site_id": site_id,
                   "window_start": format_ts(start), "window_end": format_ts(end)}
        with self.database.transaction(immediate=True) as conn:
            actor = self._actor(conn, actor_id)
            self._require(actor, "admin", "operator")
            self._site(conn, site_id)

            def create():
                self._reconcile_site(conn, site_id)
                state = self._fold(conn, site_id)
                active_task_ids = self._active_task_ids(conn, site_id)
                as_of = self._now()
                total_units = 0
                on_time_units = 0
                shipment_rows: dict[str, dict[str, Any]] = {}
                waiting_summary: dict[str, int] = {}
                for gid in sorted(state["groups"]):
                    facts = self._group_facts(state, gid)
                    first_in = facts["first_inbound_handover"]
                    if not first_in or not (format_ts(start) <= first_in < format_ts(end)):
                        continue
                    in_window_qty = sum(
                        h["quantity"] for h in state["handovers"]
                        if h["group_id"] == gid and h["stage"] == "inbound"
                        and format_ts(start) <= h["at_ts"] < format_ts(end))
                    deadline = add_minutes(parse_ts(first_in), ONE_HOUR_MINUTES)
                    ontime_qty = 0
                    for handover in state["handovers"]:
                        if handover["group_id"] != gid or handover["stage"] != "outbound":
                            continue
                        if parse_ts(handover["at_ts"]) <= deadline:
                            ontime_qty += handover["quantity"]
                    ontime_qty = min(ontime_qty, in_window_qty)
                    total_units += in_window_qty
                    on_time_units += ontime_qty
                    task = conn.execute(
                        "SELECT * FROM transfer_tasks WHERE group_id=?", (gid,)).fetchone()
                    reason = self._waiting_reason(state, facts, task, active_task_ids)
                    waiting_summary[reason] = waiting_summary.get(reason, 0) + in_window_qty
                    ship = shipment_rows.setdefault(facts["group"]["shipment_id"], {
                        "shipment_id": facts["group"]["shipment_id"],
                        "inbound_party": facts["group"]["inbound_party"],
                        "outbound_party": facts["group"]["outbound_party"],
                        "total_units": 0, "on_time_units": 0, "groups": []})
                    ship["total_units"] += in_window_qty
                    ship["on_time_units"] += ontime_qty
                    ship["groups"].append({
                        "group_id": gid, "total_units": in_window_qty,
                        "on_time_units": ontime_qty,
                        "first_inbound_handover": first_in,
                        "deadline": format_ts(deadline),
                        "last_outbound_handover": facts["last_outbound_handover"],
                        "waiting_reason": reason})
                rate = (on_time_units / total_units) if total_units else None
                detail = {"definition": "出站交接距首次进站交接不超过60分钟；"
                                        "窗口按首次进站交接归属，冻结后不再被迟到消息改写",
                          "shipments": sorted(shipment_rows.values(),
                                              key=lambda r: r["shipment_id"]),
                          "waiting_summary": dict(sorted(waiting_summary.items()))}
                report_id = f"kpi-{uuid.uuid4().hex[:16]}"
                conn.execute(
                    "INSERT INTO kpi_reports(report_id,site_id,window_start,window_end,as_of,"
                    "total_units,on_time_units,rate,detail_json,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (report_id, site_id, format_ts(start), format_ts(end), as_of, total_units,
                     on_time_units, -1.0 if rate is None else rate,
                     canonical_json(detail), self._now()))
                append_event(conn, actor_id=actor_id, action="hub.kpi.frozen",
                             resource_type="kpi_report", resource_id=report_id,
                             detail={"window_start": format_ts(start),
                                     "window_end": format_ts(end),
                                     "total_units": total_units,
                                     "on_time_units": on_time_units},
                             occurred_at=self._now())
                return {"resource_type": "kpi_report", "resource_id": report_id,
                        "report": {"report_id": report_id, "site_id": site_id,
                                   "window_start": format_ts(start),
                                   "window_end": format_ts(end), "as_of": as_of,
                                   "total_units": total_units,
                                   "on_time_units": on_time_units,
                                   "one_hour_rate": rate, **detail}}

            return self._receipt(conn, request_id=request_id, action="hub.freeze_kpi",
                                 payload=payload, create=create)

    def get_kpi_report(self, report_id: str) -> dict[str, Any]:
        """读取冻结快照：内容永远是冻结那一刻的口径，不随后续事件变化。"""
        row = self.database.connection.execute(
            "SELECT * FROM kpi_reports WHERE report_id=?", (report_id,)).fetchone()
        if row is None:
            raise NotFoundError("考核报告不存在")
        detail = json.loads(row["detail_json"])
        return {"report_id": report_id, "site_id": row["site_id"],
                "window_start": row["window_start"], "window_end": row["window_end"],
                "as_of": row["as_of"], "total_units": row["total_units"],
                "on_time_units": row["on_time_units"],
                "one_hour_rate": None if row["rate"] < 0 else row["rate"], **detail}

    def list_kpi_reports(self, site_id: str) -> list[dict[str, Any]]:
        rows = self.database.connection.execute(
            "SELECT report_id FROM kpi_reports WHERE site_id=? ORDER BY created_at,report_id",
            (site_id,)).fetchall()
        return [self.get_kpi_report(row["report_id"]) for row in rows]

    # ------------------------------------------------------------------ 经理视图

    def _waiting_reason(self, state: dict[str, Any], facts: dict[str, Any], task_row,
                        active_task_ids: set[str] | None = None) -> str:
        active_task_ids = active_task_ids or set()
        if facts["violations"]:
            return "data_exception"
        if facts["remaining"] == 0:
            return "completed" if facts["completed"] >= facts["planned"] else "dropped"
        if facts["arrived"] == 0:
            return "awaiting_arrival"
        if facts["inbound_handed"] == 0:
            return "awaiting_inbound_handover"
        if task_row is None:
            return "awaiting_scheduling"
        status = task_row["status"]
        if status == "planned":
            return "awaiting_carrier_confirmation"
        if status == "scheduled":
            return "in_operation" if task_row["task_id"] in active_task_ids else "awaiting_scheduling"
        if status == "in_progress":
            return "in_operation" if task_row["task_id"] in active_task_ids else "awaiting_rescheduling"
        if status == "pending":
            rid = self._blocking_resource_reason(state, task_row)
            return rid or "awaiting_scheduling"
        return "awaiting_scheduling"

    def _blocking_resource_reason(self, state: dict[str, Any], task_row) -> str | None:
        for outage in state["outages"]:
            if outage["start_ts"] and not outage["end_ts"]:
                return "equipment_fault"
        return None

    def _active_task_ids(self, conn, site_id: str) -> set[str]:
        """当前仍持有未结束占用的任务集合。"""
        now = self._now()
        rows = conn.execute(
            "SELECT DISTINCT task_id FROM occupancies WHERE site_id=? AND status='held' "
            "AND end_ts>?", (site_id, now)).fetchall()
        return {r["task_id"] for r in rows}

    def get_shipment_view(self, shipment_id: str) -> dict[str, Any]:
        with self.database.transaction(immediate=True) as conn:
            row = conn.execute("SELECT * FROM shipments WHERE shipment_id=?", (shipment_id,)).fetchone()
            if row is None:
                raise NotFoundError("货票不存在")
            self._reconcile_site(conn, row["site_id"])
            state = self._fold(conn, row["site_id"])
            active_task_ids = self._active_task_ids(conn, row["site_id"])
            groups_out = []
            for grow in conn.execute(
                "SELECT * FROM shipment_groups WHERE shipment_id=? ORDER BY position", (shipment_id,)
            ):
                facts = self._group_facts(state, grow["group_id"])
                task = conn.execute(
                    "SELECT * FROM transfer_tasks WHERE group_id=?", (grow["group_id"],)).fetchone()
                if facts["outbound_handed"] >= facts["arrived"] and facts["arrived"] > 0:
                    responsibility = row["outbound_party"]
                elif facts["inbound_handed"] > 0:
                    responsibility = HUB_PARTY
                else:
                    responsibility = row["inbound_party"]
                groups_out.append({
                    "group_id": grow["group_id"], "planned_qty": facts["planned"],
                    "arrived_qty": facts["arrived"], "dropped_qty": facts["dropped"],
                    "inbound_handed_qty": facts["inbound_handed"],
                    "outbound_handed_qty": facts["outbound_handed"],
                    "remaining_qty": facts["remaining"],
                    "ready_at": facts["ready_at"], "due_at": facts["due_at"],
                    "responsibility": responsibility,
                    "waiting_reason": self._waiting_reason(state, facts, task, active_task_ids),
                    "task_status": task["status"] if task else None,
                    "plan_id": task["plan_id"] if task else None,
                    "violations": facts["violations"]})
            return {"shipment_id": shipment_id, "site_id": row["site_id"],
                    "contract_id": row["contract_id"],
                    "inbound_party": row["inbound_party"],
                    "outbound_party": row["outbound_party"],
                    "custody_chain": [row["inbound_party"], HUB_PARTY, row["outbound_party"]],
                    "groups": groups_out}

    def get_timeline(self, site_id: str) -> dict[str, Any]:
        with self.database.transaction(immediate=True) as conn:
            self._site(conn, site_id)
            self._reconcile_site(conn, site_id)
            state = self._fold(conn, site_id)
            events = []
            for bid, batch in sorted(state["batches"].items()):
                events.append({"kind": "batch", "at": batch["planned_arrival"], "batch_id": bid,
                               "mode": batch["mode"], "direction": batch["direction"],
                               "planned_arrival": batch["planned_arrival"],
                               "planned_departure": batch["planned_departure"],
                               "actual_arrival": batch["actual_arrival"],
                               "actual_departure": batch["actual_departure"],
                               "closed": batch["closed"]})
            for row in conn.execute(
                "SELECT b.* FROM resource_blockades b JOIN hub_resources r ON r.resource_id=b.resource_id "
                "WHERE r.site_id=? ORDER BY b.start_ts", (site_id,)):
                events.append({"kind": "blockade", "at": row["start_ts"],
                               "blockade_id": row["blockade_id"],
                               "resource_id": row["resource_id"],
                               "start_ts": row["start_ts"], "end_ts": row["end_ts"],
                               "reason": row["reason"]})
            for outage in sorted(state["outages"], key=lambda o: (o["start_ts"] or "", o["event_id"])):
                events.append({"kind": "equipment_outage", "at": outage["start_ts"], **outage})
            for row in conn.execute(
                "SELECT * FROM occupancies WHERE site_id=? ORDER BY start_ts,occupancy_id", (site_id,)):
                events.append({"kind": "occupancy", "at": row["start_ts"],
                               "occupancy_id": row["occupancy_id"], "plan_id": row["plan_id"],
                               "task_id": row["task_id"], "group_id": row["group_id"],
                               "resource_id": row["resource_id"],
                               "start_ts": row["start_ts"], "end_ts": row["end_ts"],
                               "status": row["status"], "release_reason": row["release_reason"]})
            for handover in state["handovers"]:
                events.append({"kind": "handover", "at": handover["at_ts"], **handover})
            events.sort(key=lambda e: (e["at"] is None, e["at"] or "", e["kind"]))
            return {"site_id": site_id, "events": events}

    def list_tasks(self, site_id: str) -> dict[str, Any]:
        with self.database.transaction(immediate=True) as conn:
            self._site(conn, site_id)
            self._reconcile_site(conn, site_id)
            state = self._fold(conn, site_id)
            active_task_ids = self._active_task_ids(conn, site_id)
            items = []
            for row in conn.execute(
                "SELECT * FROM transfer_tasks WHERE site_id=? ORDER BY task_id", (site_id,)):
                facts = self._group_facts(state, row["group_id"])
                items.append({"task_id": row["task_id"], "group_id": row["group_id"],
                              "shipment_id": row["shipment_id"], "status": row["status"],
                              "plan_id": row["plan_id"],
                              "planned_qty": row["planned_qty"],
                              "completed_qty": row["completed_qty"],
                              "dropped_qty": row["dropped_qty"],
                              "remaining_qty": row["remaining_qty"],
                              "ready_at": facts["ready_at"], "due_at": facts["due_at"],
                              "waiting_reason": self._waiting_reason(state, facts, row,
                                                                     active_task_ids),
                              "violations": facts["violations"]})
            return {"site_id": site_id, "tasks": items}
