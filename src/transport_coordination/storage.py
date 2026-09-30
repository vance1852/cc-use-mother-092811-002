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
    resource_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    kind TEXT NOT NULL CHECK(kind IN ('crane', 'slot')),
    name TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_blockades (
    blockade_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES hub_resources(resource_id),
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS priority_contracts (
    contract_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    priority INTEGER NOT NULL CHECK(priority >= 0),
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    contract_id TEXT REFERENCES priority_contracts(contract_id),
    inbound_party TEXT NOT NULL REFERENCES organizations(organization_id),
    outbound_party TEXT NOT NULL REFERENCES organizations(organization_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shipment_groups (
    group_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL REFERENCES shipments(shipment_id),
    position INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    work_minutes INTEGER NOT NULL CHECK(work_minutes > 0),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    mode TEXT NOT NULL CHECK(mode IN ('rail', 'water', 'road')),
    direction TEXT NOT NULL CHECK(direction IN ('inbound', 'outbound')),
    planned_arrival TEXT NOT NULL,
    planned_departure TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_manifests (
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    group_id TEXT NOT NULL REFERENCES shipment_groups(group_id),
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    position INTEGER NOT NULL,
    PRIMARY KEY (batch_id, group_id)
);
CREATE TABLE IF NOT EXISTS hub_events (
    event_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transfer_tasks (
    task_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL UNIQUE,
    site_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    planned_qty INTEGER NOT NULL,
    completed_qty INTEGER NOT NULL DEFAULT 0,
    dropped_qty INTEGER NOT NULL DEFAULT 0,
    remaining_qty INTEGER NOT NULL,
    ready_at TEXT NOT NULL,
    due_at TEXT,
    work_minutes INTEGER NOT NULL,
    status TEXT NOT NULL,
    plan_id TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS occupancies (
    occupancy_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('held', 'released')),
    release_reason TEXT,
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('open', 'confirmed', 'rejected', 'expired', 'superseded')),
    allocation_json TEXT NOT NULL,
    allocation_hash TEXT NOT NULL,
    required_parties_json TEXT NOT NULL,
    reject_reason TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    confirmed_at TEXT
);
CREATE TABLE IF NOT EXISTS plan_confirmations (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    party TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    confirmed_at TEXT NOT NULL,
    PRIMARY KEY (plan_id, party)
);
CREATE TABLE IF NOT EXISTS kpi_reports (
    report_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    as_of TEXT NOT NULL,
    total_units INTEGER NOT NULL,
    on_time_units INTEGER NOT NULL,
    rate REAL NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
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
