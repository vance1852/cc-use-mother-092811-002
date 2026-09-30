"""换装协同测试共享的基础数据搭建。"""

from __future__ import annotations

from datetime import datetime, timezone

from transport_coordination.clock import MutableClock
from transport_coordination.hub import HubService
from transport_coordination.service import DomainService
from transport_coordination.storage import Database


def build_hub(start: datetime | None = None):
    """建立包含两个承运方、一个枢纽站点和若干资源的最小可用环境。"""
    clock = MutableClock(start or datetime(2026, 9, 29, 23, 0, tzinfo=timezone.utc))
    database = Database()
    svc = DomainService(database, clock)
    hub = HubService(database, clock)
    svc.register_organization(request_id="org-hub", actor_id="bootstrap",
                              organization_id="HUB", name="枢纽运营方")
    svc.register_actor(request_id="actor-mgr", actor_id="bootstrap", new_actor_id="mgr",
                       display_name="枢纽经理", role="admin", organization_id="HUB")
    svc.register_organization(request_id="org-rail", actor_id="mgr",
                              organization_id="RAIL", name="铁路公司")
    svc.register_organization(request_id="org-road", actor_id="mgr",
                              organization_id="ROAD", name="集卡公司")
    svc.register_organization(request_id="org-sea", actor_id="mgr",
                              organization_id="SEA", name="船公司")
    svc.register_actor(request_id="actor-rail", actor_id="mgr", new_actor_id="railop",
                       display_name="铁路值班", role="operator", organization_id="RAIL")
    svc.register_actor(request_id="actor-road", actor_id="mgr", new_actor_id="roadop",
                       display_name="集卡值班", role="operator", organization_id="ROAD")
    svc.register_actor(request_id="actor-sea", actor_id="mgr", new_actor_id="seaop",
                       display_name="船边值班", role="operator", organization_id="SEA")
    svc.register_site(request_id="site", actor_id="mgr", site_id="S1", organization_id="HUB",
                      name="重点货运枢纽", timezone_name="Asia/Shanghai")
    for rid, kind, name in (("CR1", "crane", "一号吊机"), ("CR2", "crane", "二号吊机"),
                            ("SLOT1", "slot", "甲堆位"), ("SLOT2", "slot", "乙堆位")):
        hub.register_resource(request_id=f"res-{rid}", actor_id="mgr", resource_id=rid,
                              site_id="S1", kind=kind, name=name)
    return database, svc, hub, clock
