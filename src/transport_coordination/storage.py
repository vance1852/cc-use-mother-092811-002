"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_resources (
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    resource_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('crane', 'slot')),
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(site_id, resource_id)
);
CREATE TABLE IF NOT EXISTS hub_contracts (
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    contract_id TEXT NOT NULL,
    title TEXT NOT NULL,
    priority_rank INTEGER NOT NULL CHECK(priority_rank >= 0),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(site_id, contract_id)
);
CREATE TABLE IF NOT EXISTS hub_batches (
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    batch_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('rail', 'vessel', 'vehicle')),
    planned_arrival TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(site_id, batch_id)
);
CREATE TABLE IF NOT EXISTS hub_shipments (
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    shipment_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    contract_id TEXT,
    inbound_party TEXT NOT NULL,
    outbound_party TEXT NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK(duration_minutes > 0),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(site_id, shipment_id)
);
CREATE TABLE IF NOT EXISTS hub_containers (
    site_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    container_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY(site_id, shipment_id, container_id)
);
CREATE TABLE IF NOT EXISTS hub_blockades (
    site_id TEXT NOT NULL,
    blockade_id TEXT NOT NULL PRIMARY KEY,
    resource_id TEXT NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT,
    reason TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_events (
    delivery_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    site_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    delivered_by TEXT NOT NULL,
    delivered_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_plans (
    site_id TEXT NOT NULL,
    plan_id TEXT NOT NULL PRIMARY KEY,
    supersedes TEXT,
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','superseded','lapsed','committed')),
    expires_at TEXT NOT NULL,
    committed_at TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_plan_tasks (
    plan_id TEXT NOT NULL,
    task_index INTEGER NOT NULL,
    shipment_id TEXT NOT NULL,
    group_key TEXT NOT NULL,
    container_ids_json TEXT NOT NULL,
    ready_at TEXT NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    crane_id TEXT NOT NULL,
    slot_id TEXT NOT NULL,
    PRIMARY KEY(plan_id, task_index)
);
CREATE TABLE IF NOT EXISTS hub_plan_confirms (
    plan_id TEXT NOT NULL,
    party TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    confirmed_at TEXT NOT NULL,
    PRIMARY KEY(plan_id, party)
);
CREATE TABLE IF NOT EXISTS hub_plan_releases (
    plan_id TEXT NOT NULL,
    task_index INTEGER NOT NULL,
    site_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    released_at TEXT NOT NULL,
    PRIMARY KEY(plan_id, task_index)
);
CREATE TABLE IF NOT EXISTS hub_holds (
    site_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    group_key TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hub_rate_freezes (
    site_id TEXT NOT NULL,
    freeze_id TEXT NOT NULL PRIMARY KEY,
    as_of TEXT NOT NULL,
    arrived_count INTEGER NOT NULL,
    transferred_count INTEGER NOT NULL,
    within_60m_count INTEGER NOT NULL,
    rate REAL NOT NULL,
    detail_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, as_of)
);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
