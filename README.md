# 多租户配额控制服务（tenant-quota-engine）

面向多租户场景的配额判定、用量占用与结算引擎。支持层级配额、突发借用、套餐升降级、规则版本化灰度，以及进程重启后用量不丢不重复。

## 核心语义

### 1. 配额判定（Reserve）

调用方在使用资源前先发起预留（Reserve），引擎在**一个原子操作**内完成：

1. 解析当前生效的规则版本（同一请求的所有规则来自同一不可变版本，见「规则版本化」）。
2. 构建 租户 → 团队 → 调用方 的层级链。
3. 对每个配置了规则的层级，在分片锁保护下检查 `可用额度 >= 请求量`。
4. 全部通过则更新各层级计数器并创建预留记录；任一层级额度耗尽则拒绝，并返回**可区分的拒绝原因**与耗尽的层级。

拒绝原因（`reason` 字段）：

| 原因 | 含义 |
|------|------|
| `quota_exhausted` | 某层级额度耗尽（`deny_scope` 指出具体层级） |
| `no_rule_configured` | 该租户/资源未配置规则 |
| `amount_invalid` | 请求量非法（≤0） |
| `borrow_disabled` | 借用未启用 |
| `tenant_defaulted` | 租户已违约降级 |

### 2. 预留与结算（Confirm / Cancel）

- **Confirm**：确认一笔预留，将预占额度转为实际用量。支持部分结算（`settled < amount`），差额自动释放。重复确认是幂等的。
- **Cancel**：取消一笔预留，归还已占用的全部额度。重复取消是幂等的。
- **过期清理**：预留有 TTL，后台 reaper 定期将超时未确认的预留置为 `expired` 并释放额度。

会计模型（以借用为例）：

```
Reserve(amount=80, borrowed=30)：
  子层级: reserved += 80
  父层级: lent     += 30

Confirm(settled=80)：
  子层级: reserved -= 80, used += 80, borrowed += 30
  （父层级 lent 保持，待归还）

Cancel：
  子层级: reserved -= 80
  父层级: lent     -= 30
```

任何路径（确认、取消、过期）都在分片锁内完成「检查-更新」，不会出现额度泄漏或重复扣减。

### 3. 层级配额

租户、团队、调用方形成父子约束：

- 用量同时计入所有配置了规则的上级（子级用量传播到所有祖先）。
- 判定时从叶子（调用方）到根（租户）逐层检查，**任一层级耗尽即拒绝**，拒绝优先级为 调用方 > 团队 > 租户。
- 各层级可独立配置限额与窗口。

### 4. 套餐升级与降级

通过 `SwitchPlan(tenantID, planID, mode)` 切换套餐：

- **升级（immediate）**：新额度立即生效。
- **降级（immediate）**：新额度立即生效，但**已发生的超额用量保留**——若 `used > 新额度`，新请求被拒直到当前窗口滚动，超额不会凭空消失。
- **降级（next_window）**：当前窗口继续沿用旧套餐额度，窗口滚动后自动应用新套餐。

### 5. 突发额度与借用

当子层级额度耗尽时，若规则启用了借用（`borrow_enabled`）且上级有余量，可临时向上级借用：

- 借用金额受三重上限约束：子层级缺口、子层级借用上限（`borrow_limit`）、上级可用余量。
- 借用后子层级 `borrowed` 增加（债务），上级 `lent` 增加（债权）。
- 后续窗口还款：从新窗口额度中扣减（`repayment_due`），同时恢复上级 `lent`。
- 长期还不上：超过套餐配置的宽限窗口数（`grace_windows`）后，按既定策略降级到 `downgrade_plan`，而非无限透支。

### 6. 规则版本化与灰度

- 每次发布生成不可变版本（draft → active/gray）。
- 灰度发布：按调用方 ID 的稳定哈希（FNV）取模，命中灰度百分比的调用方看到新版本，其余仍读旧版本。
- **自洽性**：单次请求解析出的所有规则共享同一版本号，不会出现新旧规则混用。
- 宽限期：灰度期间旧版本继续可读，支持回滚（Rollback）。

### 7. 可观测性

- 按租户/层级/资源查询当前用量、剩余额度、拒绝次数。
- 所有指标从持久化计数器派生，进程重启后通过 WAL 重放恢复，**不丢不翻倍**。

## 持久化

`FileStore` 采用 append-only WAL：

- 每次变更先 fsync 追加到 WAL，再更新内存。
- 启动时重放 WAL 恢复状态。
- 定期 Snapshot 压缩日志（全量快照 + 截断 WAL）。

## 配置项

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `default_ttl` | 5m | 预留默认有效期 |
| `reap_interval` | 30s | 后台清理间隔 |
| `shard_count` | 64 | 分片锁数量（2 的幂） |

规则字段：

| 字段 | 说明 |
|------|------|
| `scope` | 生效层级（tenant/team/caller + id） |
| `resource` | 资源维度 |
| `limit` | 窗口限额 |
| `window` | 时间窗口（如 `1m`、`1h`） |
| `borrow_enabled` | 是否允许借用 |
| `borrow_limit` | 最大未还借用额 |

套餐字段：

| 字段 | 说明 |
|------|------|
| `quotas` | 套餐包含的规则列表 |
| `grace_windows` | 违约宽限窗口数 |
| `repay_ratio` | 每窗口最多还款比例（0~1） |
| `downgrade_plan` | 违约后降级到的套餐 |

## HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/reserve` | 预留额度 |
| POST | `/v1/confirm` | 确认结算 |
| POST | `/v1/cancel` | 取消预留 |
| POST | `/v1/plans/switch` | 套餐切换 |
| GET | `/v1/usage` | 查询用量 |
| GET/POST | `/v1/rules/versions` | 列出/发布规则版本 |
| POST | `/v1/rules/activate` | 激活/灰度规则版本 |
| GET | `/v1/health` | 健康检查 |

## 构建与运行

```bash
go build ./...
go test ./...
go run ./cmd/quota-server -addr :8080 -data-dir ./data
```

## 项目结构

```
internal/
  model/      领域类型
  store/      存储接口、内存实现、WAL 文件持久化
  engine/     配额核心（预留/确认/取消、层级、套餐、借用、清理）
  rules/      规则版本化与灰度
  observe/    用量与拒绝指标
  api/        HTTP 接口
cmd/quota-server/  服务入口
```
