# tenant-quota-engine-go

面向多租户的配额控制服务。在并发场景下保证配额判定、用量占用与结算、
套餐等级变更的准确与一致：同一窗口内的用量统计既不超过也不低于真实值，
任何路径都不出现额度泄漏或重复扣减。

## 快速开始

```go
e, _ := quota.NewEngine(quota.Options{
    DataDir:        "/var/lib/quota",      // 启用 WAL + 快照持久化
    ReservationTTL: time.Minute,           // 预留过期自动归还
    Plans: []quota.Plan{
        {Name: "free", Rank: 1, Window: quota.WindowMinute, Limits: map[string]int64{"api": 100}},
        {Name: "pro",  Rank: 2, Window: quota.WindowMinute, Limits: map[string]int64{"api": 1000}},
    },
    Borrow:     quota.BorrowPolicy{Enabled: true, MaxBorrowRatio: 0.5, MaxOverdueWindows: 2},
    PoolLimits: map[string]int64{"api": 100000},
})
defer e.Close()

e.UpdateRules(quota.RuleSet{
    Version: 1, GrayPercent: 100, GracePeriod: time.Minute,
    Rules: []quota.Rule{
        {Scope: quota.ScopeTenant, Resource: "api", Window: quota.WindowMinute, Limit: 100},
        {Scope: quota.ScopeTeam,   Resource: "api", Window: quota.WindowMinute, Limit: 60},
        {Scope: quota.ScopeCaller, Resource: "api", Window: quota.WindowMinute, Limit: 30},
    },
})

// 占用 -> 结算 / 归还
res, r := e.Reserve(ctx, quota.CheckRequest{
    Subject:  quota.Subject{TenantID: "t1", TeamID: "g1", CallerID: "c1"},
    Resource: "api", Amount: 1,
})
if r.Allowed() {
    defer e.Release(ctx, res.ID) // 失败路径必须归还
    // ... 执行业务调用 ...
    e.Commit(ctx, res.ID)        // 成功后结算
}
```

## 配额语义

### 判定与拒绝原因

`Check` / `Reserve` 的判定结果只有允许或拒绝两种，拒绝原因可区分：

| 原因 | 含义 |
|---|---|
| `TENANT_QUOTA_EXCEEDED` | 租户层额度耗尽 |
| `TEAM_QUOTA_EXCEEDED` | 团队层额度耗尽 |
| `CALLER_QUOTA_EXCEEDED` | 调用方层额度耗尽 |
| `BORROW_LIMIT_EXCEEDED` | 借用比例超限或资源池余量不足 |
| `NO_MATCHING_RULE` | 未发布规则集或无匹配规则 |
| `INVALID_RESERVATION` | 预留不存在或已终结（防重复扣减） |
| `PLAN_DOWNGRADED_OVERDUE_BORROW` | 借用长期逾期被降级 |

时间窗口为固定长度的滚动窗口（`WindowMinute` / `WindowHour` / `WindowDay` 或自定义
`Window{Size}`）。判定与占用在引擎内被序列化为原子操作，并发下不超发也不少计。

### 预留与结算

- `Reserve` 判定通过即占用额度，返回带 TTL 的预留单；
- `Commit` 把预留转为实际用量，`Release` 归还占用；
- 重复 `Commit` / `Release` 返回 `INVALID_RESERVATION`，不会重复扣减或重复返还；
- 过期未结算的预留在下一次触及该窗口时被惰性回收，额度自动归还，绝不泄漏。

### 层级配额

租户 > 团队 > 调用方 三级父子约束。子级的每一笔占用同时计入所有上级；
任一层耗尽即拒绝，拒绝优先级为父级优先（租户 > 团队 > 调用方）。
某一层没有配置规则时该层不约束。

### 套餐升级与降级

套餐按 `Rank` 排序。窗口中段切换套餐时，当前窗口的生效额度按时间折算：

```
生效额度 = 旧额度 × 已过比例 + 新额度 × 剩余比例
```

- **升级**：折算后额度立即高于旧额度，新增额度当窗口即可用；
- **降级**：折算平滑收紧；若已用量超过折算额度，生效额度被抬升到已用量——
  已发生的超额用量不凭空消失，但剩余为 0，后续请求被拒绝直到下一窗口。

### 突发额度与借用

`BorrowPolicy` 开启后，租户层额度不足时可从上级资源池（`PoolLimits`）临时借用：

- 单窗口累计借用不超过 `自身额度 × MaxBorrowRatio`，且资源池必须有足够余量；
- 只有借用能完全覆盖缺口才放行，避免部分借用的模糊语义；
- 进入新窗口时自动用本窗口额度的空余部分归还历史欠款（归还占用本窗口额度）；
- 归还不足记一次逾期，连续 `MaxOverdueWindows` 个窗口未还清自动降级一档套餐，
  绝不无限透支；未结算即释放的借用直接冲销，不计入欠款。

### 规则版本化与灰度

- `UpdateRules` 发布不可变的规则集快照，读取方永远看到某个完整版本；
- `GrayPercent` 按租户哈希稳定分流（同一租户多次判定结果一致），
  未命中灰度的租户回退到次新版本；
- 请求可通过 `CheckRequest.RuleVersion` 钉住旧版本，
  在该版本的 `GracePeriod` 内仍按旧版本判定，过期后自动切换到最新版本。

### 可观测性与持久化

`Usage(tenant, resource, now)` 返回当前窗口的已用量、预留量、生效额度、剩余额度、
累计放行次数、按原因分类的拒绝次数以及未归还借用总额。

启用 `DataDir` 后，所有状态变更先写 WAL 再应用，每 512 条事件（及 `Close` 时）
原子地写全量快照并截断 WAL；重启时加载快照并按序号精确重放一次增量事件，
统计结果不丢失、不翻倍。

## 配置项（Options）

| 字段 | 说明 |
|---|---|
| `DataDir` | 持久化目录，空为纯内存 |
| `ReservationTTL` | 预留过期时间，默认 1 分钟 |
| `Plans` | 套餐表：`Name` / `Rank` / `Window` / `Limits` |
| `Borrow` | 借用策略：`Enabled` / `MaxBorrowRatio` / `MaxOverdueWindows` |
| `PoolLimits` | 上级资源池各资源在租户窗口内的总额度 |
| `Now` | 可注入时钟（测试用），空为真实时间 |

## 测试

```sh
go test ./... -race
```

覆盖：并发争用（不超发不少计）、预留泄漏与重复结算、层级约束与拒绝优先级、
套餐升降级窗口折算、借用归还与逾期自动降级、规则灰度与宽限期、
重启持久化不丢失不翻倍。
