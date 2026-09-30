# 重点货运枢纽多式联换装协同服务

面向重点货运枢纽“一小时换装率”考核的后端协同服务。它把**列车、船舶、集疏运车辆**的批次
到达、**箱组约束、吊机/堆位等作业资源、封锁窗口、优先合同**放进同一条统一时间线，
先产出**有期限的换装方案**，在各承运方逐一确认后于单事务内**原子占用能力**；
提前、延误、甩箱、部分完成、设备故障只会推动**受影响任务重排**，
已交接货物不回退；经理可从 API 看到每票货的交接责任、关键等待原因和冻结口径下的换装率。

运行时仅依赖 Python 标准库与 SQLite，内置角色权限、请求幂等、事务边界与哈希串联审计。

## 核心机制

### 统一时间线与两阶段承诺

1. **登记静态事实**：作业资源（吊机 `crane`/堆位 `slot`）、资源封锁窗口、优先合同、
   货票（shipment，含若干箱组 group 与单组作业工时）、批次（列车/船舶/车辆，含方向与箱组清单）。
2. **有期限方案**：`POST /plans` 在给定窗口内按“合同优先级 → 就绪时刻 → 货票 → 任务”
   贪心排程，方案带 `expires_at`（默认 30 分钟），**开口期不占用任何能力**；
   排不进窗口的任务进入 `unscheduled` 并给出原因。同一站点只允许一个在途方案，
   新建方案自动作废旧开口方案。
3. **承运方逐方确认**：`POST /plans/confirm` 由各方组织分别确认；
   全部确认的瞬间在**单个事务**内重新校验封锁、故障、已占用、就绪时刻与剩余数量，
   通过则原子写入全部吊机/堆位占用；任一冲突则**整单拒绝、零占用写入**。
   超时未确认的方案自动失效。

### 确定性事件折叠与数量守恒

所有动态变化都是**只追加事件**（`POST /events`）：列车/船舶到达与提前延误
（`arrival`）、分批到达量（`arrival_qty`）、甩箱（`drop_qty`）、
批次出发（`batch_departure`）、设备故障与恢复（`equipment_fault`/`equipment_recovered`）、
进站/出站交接（`handover`）。

* 折叠严格按 `(occurred_at, event_id)` 的规范顺序，**乱序投递与重复重放结果一致**：
  恢复消息先于故障消息也能正确闭合故障区间。
* `event_id` 是自然幂等键，同一编号同内容即重放、不同内容即冲突。
* 到达量/甩箱量按**累计最大值**聚合（单调不降），交接流水只追加；
  上报量超过清单自动钳制，交接量不可能超过到达量，违规则进入 `exception`。
* **已交接货物不回退到未到达**：进站交接后责任转移到枢纽，后续列车延误消息不影响该货。

### 受影响重排

事件到达后重算全部任务：命中故障/封锁/延误/甩箱/完成的任务，
其**所有未结束**占用（吊机+堆位）成组释放、任务回待排，其它任务的占用保持不动；
已结束的历史占用永不释放。随后可再次 `POST /plans` 对剩余量重排。

### 交接责任、等待原因与冻结考核

* `GET /shipments/{id}`：每票货的 custody chain（进站承运方 → HUB → 出站承运方）、
  每个箱组的计划/到达/甩箱/进站交接/出站交接/剩余数量、当前责任方与关键等待原因
  （`awaiting_arrival`、`awaiting_inbound_handover`、`awaiting_carrier_confirmation`、
  `in_operation`、`equipment_fault`、`awaiting_rescheduling`、`completed`、`dropped` 等）。
* `GET /timeline?site_id=`：批次、封锁、设备故障、占用、交接的统一时间线。
* `POST /kpi-reports`：按窗口生成**冻结快照**。口径为：窗口内首次进站交接的箱量为分母，
  其中自首次进站交接起 60 分钟内完成出站交接的箱量为分子。快照持久化，
  **冻结后迟到的消息不改变旧报告**；重新冻结才产生反映新事实的新报告。
* 所有时间以 UTC ISO8601（Z）存储，按分钟调度，跨午夜计划无日期回绕问题。

## 目录

- `src/transport_coordination/`
  - `hub.py`：换装协同核心（登记、事件折叠、两阶段计划、原子占用、受影响重排、冻结考核）
  - `timeutil.py`：UTC 时间解析、分钟运算、半开区间重叠判定
  - `clock.py`：`SystemClock` / `FixedClock` / 可推进的 `MutableClock`
  - `service.py`、`storage.py`、`audit.py`、`models.py`、`api.py`：基础登记、SQLite 事务、审计链、HTTP 边界
  - `acceptance.py`：基础服务离线验收；`acceptance_hub.py`：换装协同离线验收（跨日+恢复）
- `tests/`：基础规则、换装计划/事件/重排/HTTP 与两套端到端验收测试

## 环境

- Linux，Python 3.11+，仅使用标准库与 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m transport_coordination.acceptance      # 基础服务
PYTHONPATH=src python3 -m transport_coordination.acceptance_hub  # 换装协同
```

换装协同验收使用可控时钟跨越午夜，覆盖：列车提前到达、有期限方案、双方确认原子占用、
吊机故障只重排受影响任务、一小时内交接、冻结 KPI、关库重开后占用/任务/报告/审计链完全一致。
成功时输出一行 `status` 为 `ok` 的 JSON，退出码 0。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m transport_coordination.api --database hub.sqlite3 --host 127.0.0.1 --port 8080
```

写入接口通过 `X-Actor-Id` 标识操作者；服务重启后 SQLite 中的事件、占用、方案与冻结报告继续保留。

### 换装协同接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/hub/resources` | 登记吊机/堆位 |
| POST | `/hub/blockades` | 登记资源封锁窗口（立即参与冲突判定） |
| POST | `/hub/contracts` | 登记优先合同（priority 越小越优先） |
| POST | `/shipments` | 登记货票与箱组（箱量、作业工时） |
| POST | `/batches` | 登记列车/船舶/车辆批次与箱组清单 |
| POST | `/events` | 追加到达/甩箱/故障/交接等事件（乱序、重放安全） |
| POST | `/plans` | 生成带期限的换装方案（不占能力） |
| POST | `/plans/confirm` | 承运方逐方确认；末次确认原子占用，冲突时 409 整单拒绝 |
| POST | `/kpi-reports` | 冻结一小时换装率考核快照 |
| GET | `/shipments/{id}` | 每票货交接责任、数量、关键等待原因 |
| GET | `/timeline?site_id=` | 统一时间线 |
| GET | `/tasks?site_id=` | 换装任务状态与等待原因 |
| GET | `/kpi-reports?site_id=`、`/kpi-reports/{id}` | 冻结报告列表与快照详情 |

### 典型调用顺序

```
登记资源/封锁/合同 → 登记货票箱组 → 登记到达与出发批次
→ 上报到达/到达量/进站交接事件 → POST /plans 出方案
→ 各承运方 POST /plans/confirm（末次成功即原子占用）
→ 故障/延误/甩箱/部分交接事件 → 受影响任务自动释放重排 → 再次出方案确认
→ POST /kpi-reports 冻结考核 → GET /shipments/{id} 查看责任与等待原因
```
