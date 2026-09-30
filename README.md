# 多式联运一小时换装协同服务

面向重点货运枢纽考核的一小时换装率协同系统。把列车、船舶、集疏运车辆的
批次到达、箱组约束、吊机/堆位作业资源、封锁窗口与优先合同放进同一条
统一时间线：先生成**有期限的换装方案**，各承运方确认后再**原子占用**
吊机与堆位；提前、延误、甩箱、部分完成和设备故障只会推动**受影响任务**
重排，已交接货物不回退；相同事件重放返回原结果，乱序消息不破坏数量守恒。

## 核心规则

- **统一时间线**：所有外部消息归一化为事件（`arrival.early/confirmed/delayed`、
  `arrival.skipped`、`work.partial`、`equipment.failed/recovered`），按
  `(occurred_at, sequence, event_id)` 确定性排序后折叠，消息入库顺序不影响结果。
- **有期限方案 → 承运方确认 → 原子占用**：提案含 `expires_at`，到期自动
  `lapsed`；方案涉及的交出方/接收方全部确认后，提交时在最新时间线上复验
  可行性，通过后同事务写入吊机+堆位占用，杜绝同一吊机与堆位被重复承诺。
- **只重排受影响任务**：到达类事件只取“最新一条”预测；甩箱收缩箱组；
  设备故障只释放与故障窗口重叠且未交接的任务，其余任务与占用保持不变。
- **交接单调、数量守恒**：未到达不能交接，已交接不能甩箱或回退为未到达；
  每票恒有 `登记 = 在途 + 到达待转 + 已交接 + 甩箱`，任何事件入库后全量校验。
- **重放幂等**：`request_id` 与 `event_id` 双重去重；相同事件（即使文本
  形式不同）归一化后摘要一致，重放返回首次结果。
- **冻结口径换装率**：分母为截至时点**实际到达**的箱（甩箱永不计入），
  一小时内完成的箱计入口径内；`rate-freezes` 把某时点结果固化用于考核追溯。
- **可控时钟**：`ManualClock` 支持跨日计划、方案到期、故障恢复等离线测试。

## 目录

- `src/transport_coordination/`
  - `hub_models.py` / `hub_events.py`：数据对象与事件归一化校验；
  - `hub_planning.py`：确定性折叠投影、箱组任务推导、贪心排程、等待原因诊断；
  - `hub_service.py`：登记、时间线、两阶段方案、原子占用、重排与冻结口径；
  - `service.py` / `storage.py` / `audit.py` / `clock.py` / `api.py`：
    主体权限、SQLite 事务、哈希审计链、时钟与 HTTP 边界。
- `tests/`：基础能力、换装协同、HTTP 路由、持久化恢复与离线验收测试。

## 环境

- Linux，Python 3.11+，仅依赖标准库与 SQLite。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m transport_coordination.acceptance
```

验收剧本使用 `ManualClock` 驱动跨日场景：登记两吊机/两堆位、三方承运方、
优先合同与封锁窗口；验证有期限提案失效、三方确认后原子占用、提前/延误/
甩箱/故障的精准重排、已交接不回退、相同事件重放与乱序消息口径一致、
数量守恒以及 `0.75` 的冻结换装率。成功输出一行 `status` 为 `ok` 的 JSON。

## HTTP 接口

```bash
PYTHONPATH=src python3 -m transport_coordination.api \
  --database transport_coordination.sqlite3 --host 127.0.0.1 --port 8080
```

写入接口通过 `X-Actor-Id` 标识操作者，所有写操作支持 `request_id` 幂等。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/hub/resources` | 登记吊机 `crane` / 堆位 `slot` |
| POST | `/hub/contracts` | 登记优先合同（`priority_rank` 越小越优先） |
| POST | `/hub/batches` | 登记列车/船舶/车辆批次与计划到达 |
| POST | `/hub/shipments` | 登记货物、交接承运方、箱组与单件箱序 |
| POST | `/hub/blockades` | 登记资源封锁窗口 |
| POST | `/hub/events` | 承运方消息进入统一时间线并触发受影响重排 |
| POST | `/hub/plans` | 生成有期限换装方案（可指定 `valid_minutes`） |
| POST | `/hub/plans/confirm` | 承运方确认方案 |
| POST | `/hub/plans/commit` | 确认齐备后原子占用 |
| GET | `/hub/plans?plan_id=` | 方案状态、任务时间窗与确认情况 |
| GET | `/hub/shipments?site_id=` | 每票货物的交接责任、等待原因与时间窗 |
| GET | `/hub/shipment?site_id=&shipment_id=` | 单票货物详情 |
| GET | `/hub/timeline?site_id=` | 确定性排序后的时间线 |
| GET | `/hub/conservation?site_id=` | 数量守恒核对 |
| GET | `/hub/rate?site_id=&as_of=` | 一小时换装率（可指定时点） |
| POST | `/hub/rate-freezes` | 冻结某时点口径 |
| GET | `/hub/rate-freezes?site_id=` | 冻结历史 |

枢纽经理可从货物视图看到 `handover_stage`（交接责任阶段）、`custodian`
（当前责任方）、`waiting_reason`（`waiting_arrival` / `equipment_failed` /
`blockade_window` / `resource_contention`）以及方案时间窗。
