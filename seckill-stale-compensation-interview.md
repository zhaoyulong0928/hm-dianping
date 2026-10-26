# 秒杀项目难点：旧补偿迟到，为什么会误撤销新订单？

> 适用场景：Java 后端暑期实习面试、项目复盘。
>
> 贡献说明：本项目代码、测试和文档有 AI 辅助。本文不用于证明“我独立设计并实现全部方案”。面试前，我需要能够结合代码解释问题、方案取舍、测试证据和剩余边界，而不是只记住结论。

## 一、业务背景

项目的秒杀下单大致分成两步：

1. `VoucherOrderServiceImpl.seckillVoucher()` 调用 `seckill.lua`，在 Redis 中完成时间、库存和一人一单校验，然后预扣库存并写入占坑。
2. Java 发送 RabbitMQ 消息，由 `onOrderMessage()` 消费并写入 MySQL 订单、扣减数据库库存。

如果下单失败，就需要补偿：把 Redis 库存加回来，并删除用户占坑，让用户可以重试。

项目中有两类补偿来源：

- 生产发送失败、消费终态失败：在 `VoucherOrderServiceImpl` 中调用 `rollbackSeckill()`，token 形如 `order:{orderId}`。
- 定时对账：`SeckillReconciliationTask` 发现“Redis 有占坑，但长时间没有数据库订单”后补偿，token 形如 `recon:{voucherId}:{userId}`。

### 通俗理解：旧请求和新请求共用了一个“用户占坑”

可以把占坑理解为一张临时占位票。修复前，这张票上只写了“哪个用户”，没有写“属于哪笔订单”。

于是可能出现下面的顺序。假设初始库存为 1：

| 顺序 | 发生的操作 | Redis 库存 | 用户占坑 |
|---|---|---:|---|
| 1 | 旧请求 A 预扣 | 0 | 有，本应属于 A |
| 2 | A 的一条补偿路径先执行 | 1 | 无 |
| 3 | 同用户重新抢购，产生新请求 B | 0 | 有，本应属于 B |
| 4 | A 的另一条补偿路径迟到 | **1** | **被删除，但实际删除的是 B 的占坑** |

问题就在于：第 4 步本来想撤销 A，却因为 Redis 只知道“这个用户有占坑”，误把 B 的占坑当成 A 的处理了。

## 二、问题发现

第二阶段测试没有依赖随机并发，而是在 `SeckillCompensationRaceTest` 中按确定顺序直接调用业务方法和补偿方法：

```text
旧请求预扣
→ 旧请求首次补偿
→ 同用户重新抢购
→ 同 token 的旧补偿重试
→ 另一旧路径 token 的补偿迟到
```

修复前的实际结果是：

- 同 token 重试返回 `false`，没有重复补偿，符合预期。
- 另一旧路径 token 返回 `true`。
- Redis 库存从 0 变为 1。
- 新请求的占坑被移除。

关键实测数据为：voucher=26，oldOrder=9200000000001，newOrder=652906875204928490。

```text
firstCompensation=true
sameTokenLate=false
otherTokenLate=true
stockAfterLate=1
occupiedAfterLate=false
```

**证据边界：**该测试使用真实 Redis，但 mock 了 MQ 生产者，并手动安排调用顺序。它证明了代码在这种状态和顺序下确实会误回滚，不等于已经通过自然调度复现了所有生产时序。

## 三、根因分析

### 1. token 去重和请求归属是两件事

修复前的 `rollbackSeckill.lua` 主要检查：

1. 用户当前是否还在占坑。
2. 当前 `rollbackToken` 是否已经使用过。

token 只能回答：

> 这个补偿动作以前执行过吗？

它不能回答：

> 当前这个占坑，是否仍然属于我准备补偿的那笔订单？

同 token 重试可以被 `SET NX` 拦截，但订单路径和对账路径使用不同 token。一个未使用过的旧 token，在新请求已经占坑后，仍可能通过检查。

### 2. Set 只保存 userId，无法区分 A 和 B

修复前的占坑结构相当于：

```text
seckill:order:{voucherId} = Set<userId>
```

`SISMEMBER` 可以支持一人一单，却无法区分这次占坑属于 A 还是后来重新抢购的 B。

所以，真正缺少的是“请求归属”信息，而不是再增加一个 token 名称。

### 3. 为什么归属比较和回滚必须在同一 Lua 中？

如果 Java 先读取 orderId，再分几条命令判断和回滚，会出现新的空档：

```text
Java 读到当前占坑属于 A
→ 其他操作改变占坑状态
→ Java 仍按旧结果继续加库存、删占坑
```

因此，`HGET` 获取当前订单、字符串比较、token 幂等检查、`INCRBY` 和 `HDEL` 都应在同一个 `rollbackSeckill.lua` 中完成。这里的原子性指当前项目所使用的单机 Redis Lua 执行模型，不能据此推断 Redis Cluster 也已经可用。

## 四、方案选择

### 1. 为什么不只是更换 token？

只改 token 前缀或命名方式，仍然没有告诉补偿逻辑“当前占坑属于谁”。

即使把不同路径的 token 统一成订单维度，也仍然需要让对账任务知道当前 orderId，并处理旧补偿与新占坑之间的关系。因此，订单身份归属是核心，token 只负责同一补偿来源的幂等。

### 2. 为什么选择 Hash 保存 userId → orderId？

将原来的 key `seckill:order:{voucherId}` 改为：

```text
Hash field = userId
Hash value = orderId
```

这样可以复用现有 key 和一人一单模型：

- `HEXISTS` 继续负责判断用户是否已占坑。
- `HSET` 在预扣时同时保存订单归属。
- `HGET` 在补偿前读取当前订单。
- `HDEL` 在归属一致时删除占坑。

它不需要新增数据库表，也不需要额外维护一套订单状态机。相比“Set 再配一个单独订单 key”，Hash 还避免了两种结构之间的同步管理。

### 3. 为什么复用 orderId，而不增加新锁或版本号？

`seckill.lua` 原本就已经接收 `orderId` 参数，只是旧实现没有把它写入占坑。直接复用它，改动最小，也足以区分同一用户同一张券的新旧请求。

增加新锁或新表可以解决一部分协调问题，但如果补偿仍不检查订单归属，单靠加锁并不能自动保证正确性。对这个实习项目而言，先补足最基本的数据语义，比引入更多组件更合理。

### 4. Set 改 Hash 有什么代价？

Redis 的 Set 和 Hash 是不同类型，不能对同一个 key 混用两套命令，否则会出现 `WRONGTYPE`。

本次只在空的专用 Redis DB 15 中完成切换，并提前确认订单队列、DLQ 和消费者均已静止，没有执行旧 Set 数据的在线迁移。

因此，不能把本次结果直接表述为“已经完成生产无损迁移”。已有数据环境仍需在静止窗口清理或迁移相关 key。

## 五、具体修复

### 1. 预扣时保存订单归属

`seckill.lua` 的核心变化是：

```lua
if(redis.call('hexists', orderKey, userId) == 1) then
    return 2
end

redis.call('incrby', stockKey, -1)
redis.call('hset', orderKey, userId, orderId)
```

orderId 在 Redis 和 Lua 中始终按字符串保存和比较，不转换成 Lua 数值。这也是为了避免长整型订单 ID 转成浮点数值后产生精度风险。

### 2. 补偿时先校验归属，再幂等回滚

`rollbackSeckill.lua` 的核心逻辑为：

```lua
local currentOrderId = redis.call('hget', KEYS[3], ARGV[1])
if not currentOrderId or currentOrderId ~= ARGV[2] then
    return 0
end

if not redis.call('set', KEYS[1], '1', 'EX', tonumber(ARGV[3]), 'NX') then
    return 0
end

redis.call('incrby', KEYS[2], 1)
redis.call('hdel', KEYS[3], ARGV[1])
return 1
```

`IVoucherOrderService.rollbackSeckill()` 相应增加 `String expectedOrderId` 参数：

- 生产发送失败传入当前 orderId。
- 消费终态补偿传入 `msg.id`。
- 对账补偿传入占坑 Hash 中读取的 orderId。

这样，A 的补偿迟到时，如果当前占坑已经属于 B，就会直接返回 0，不再恢复库存或删除 B 的占坑。

### 3. 对账 suspect 也按订单隔离

仅修改回滚脚本还不够。对账任务还会保存“第一次发现异常的时间”。

原 suspect key 只有 `{vid}:{uid}`，可能出现：

- A 的 suspect 已存在很久。
- A 被补偿后，B 重新占坑。
- B 沿用 A 的时间，过早满足宽限期。
- 或者旧订单清理 suspect 时，把 B 的 suspect 一并删掉。

现在 `SeckillReconciliationTask` 使用：

```text
seckill:recon:suspect:{vid}:{uid}:{orderId}
```

效果是：

- A 和 B 分别记录首次发现时间。
- 清理 A 的 suspect 不会删除 B 的 suspect。
- 旧 suspect 等 TTL 自然清理即可。

同时，`reconcileVoucher()` 仍然查询数据库判断订单是否已经成功。orderId 归属校验只限制补偿对象，没有替代“订单是否已成功落库”的业务判断。

## 六、测试验证

### 实际执行结果

修复回归报告记录了最终 14 个不同用例全部实际通过：第一阶段 8 项、第二阶段原有 5 项，以及新增 suspect 隔离 1 项。

| 验证内容 | 实际结果 |
|---|---|
| 原失败竞态，断言不变 | sameTokenLate=false、otherTokenLate=false、stock=0、occupied=true |
| 生产发送失败补偿 | 正常恢复一次；同 token、跨 token 重复补偿均未重复加库存 |
| 对账幂等 | 首见只标记，到期补偿一次，重复对账不多加库存 |
| 新旧订单 suspect 隔离 | lateOldCompensation=false，旧 suspect 保留，新 suspect 独立更新和清理 |
| 事务回滚与死信 | INSERT 返回后注入 UPDATE 失败；实际尝试 3 次，订单回滚，死信身份匹配 |
| 消费重试 | 第 3 次实际执行成功，只落一单，DLQ 为空 |
| 并发与重复消息 | 1000 用户抢 100 库存，落 100 单；5 条重复消息只落 1 单 |
| HTTP 全链路及边界 | HTTP 正常链路、时间、库存和重复抢购断言均通过 |

原竞态修复后的实测数据包括：voucher=27，oldOrder=9200000000001，newOrder=652959750547308545。

### 代码推断，不当作实测结论

- 如果新订单 B 的占坑被误删，后续消费者可能跳过落库，或者出现 Redis 与数据库库存不一致。这是根据消费代码作出的推断，不是原竞态测试直接验证的结果。
- 两条补偿路径在自然运行中可能因阻塞、调度延迟而形成先后顺序；本次测试是手动构造顺序，没有证明所有自然时序都会发生。

## 七、剩余边界

本次结果不能扩大为“解决所有竞态”“绝对不超卖”或“达到生产级可靠性”。仍未覆盖：

- 已有 Set 占坑数据和旧格式 suspect 的在线迁移。
- Publisher confirm nack/return、进程或 Broker 崩溃。
- Redis Cluster、多实例对账和长期压测。
- 死信重放与人工恢复、非空 DLQ 下的对账守卫。
- `count` 前置查重分支的单独验证。

此外，本次只针对“旧补偿误撤销新占坑”做最小修复，不代表其他消费和对账竞态都已验证。

---

## 八、1 分钟口述稿

我在项目中参与排查过一个秒杀补偿竞态。系统先在 Redis 预扣库存并记录用户占坑，失败时再补偿。原来的占坑是 Set，只保存 userId；补偿则检查用户是否占坑以及 token 是否用过。

问题在于，token 去重只能防止同一个补偿动作重复执行，不能证明当前占坑仍属于旧订单。旧订单 A 补偿后，用户马上重新抢购得到 B。如果 A 的另一条补偿路径使用不同 token 迟到，就可能把 B 的库存和占坑错误回滚。第二阶段用固定顺序测试实际复现了这个问题。

最小修复是把占坑改成 Hash，保存 userId 到 orderId 的映射。回滚 Lua 先比较当前订单和 expectedOrderId，再在同一个脚本里完成 token 幂等、库存恢复和占坑删除。对账 suspect 也追加 orderId，避免新订单继承旧订单的嫌疑时间或被误清理。修复后，原失败用例在不改断言的情况下通过，相关 14 项回归全部通过。但在线迁移和其他竞态还没有验证，所以我不会把它描述成生产级可靠性方案。

## 九、3 分钟展开稿

这个难点来自秒杀的“Redis 预扣 + MQ 异步落库”模式。正常请求先通过 Lua 扣 Redis 库存并写占坑，然后发 MQ，由消费者写 MySQL。失败时需要把 Redis 预扣撤销；定时对账也会处理长期有占坑但没有订单的情况。

我关注的问题是：同一个请求可能有不同来源的补偿，而补偿之间可能隔着一次重新抢购。旧请求 A 先预扣，某条路径把 A 补偿掉，库存恢复且占坑删除。用户随后生成新请求 B，再次扣库存并占坑。如果 A 的另一条补偿路径这时才到，就会发生归属误判。

修复前，占坑 Set 里只有 userId，回滚 Lua 只检查用户是否占坑和 token 是否用过。token 的设计能解决同一来源重复执行，却不能识别当前占坑是 A 还是 B。测试也正好体现了这一点：同 token 的迟到补偿被挡住，但另一旧路径 token 返回成功，导致库存多恢复 1，B 的占坑被删除。

方案上，我没有继续增加 token 或引入新锁，而是补上订单身份。因为 `seckill.lua` 本来就有 orderId 参数，所以把占坑改为 Hash，field 是 userId，value 是字符串 orderId。这样正常一人一单仍然用 `HEXISTS`，补偿则先用 `HGET` 取出当前订单，再和 expectedOrderId 做字符串比较。

归属检查和回滚必须放在同一个 Lua 中。如果 Java 先查再改，查询之后状态仍可能变化，检查就失去了意义。脚本里还会保留原来的 token `SET NX` 幂等闸门，只有归属匹配时才恢复库存和删除占坑。

对账也需要同步考虑。suspect 原来只按用户和券记录，A 的旧时间可能被 B 复用，清理时也可能误删。于是 suspect key 追加 orderId，让新旧订单分别计算宽限期、分别清理；数据库是否已经成功落单的查询仍然保留。

验证方面，原失败用例没有放宽断言，修复后两种旧补偿都返回 false，B 的库存保持 0、占坑保留。新增 suspect 隔离用例也通过，第一阶段和第二阶段相关回归合计 14 项通过。

这个方案仍有边界：Set 改 Hash 有类型兼容问题，本次只在空的专用 Redis 中切换，没有验证在线迁移；自然调度下的所有竞态、Redis Cluster、多实例对账和长期压测也没有覆盖。我会把它定位为针对一个已复现问题的最小修复，而不是宣称整个秒杀链路已经达到生产级可靠。

---

## 十、面试追问与回答要点

### 1. 已经有 token 幂等，为什么还会重复加库存？

token 只识别“这个补偿动作是否执行过”，不识别“当前占坑属于哪个订单”。不同路径 token 不同，新请求又复用了同一用户占坑，所以旧 token 仍可能通过检查。需要额外的 expectedOrderId 归属校验。

### 2. 为什么用 Hash，而不是继续用 Set？

Set 只能表达用户是否存在，不能保存该次占坑的订单 ID。Hash 可以在原来的 key 上保存 userId → orderId，同时支持一人一单和归属校验，改动较小，也不用维护额外的映射 key。

### 3. 为什么不能在 Java 中先查 orderId，再决定回滚？

因为查和改之间状态可能变化。Java 读到 A 后，另一个线程可能已经处理 A 并让用户创建 B。如果 Java 仍按旧结果回滚，又会误删 B。把读取、比较和回滚放进同一 Lua，是为了消除这个检查与执行之间的空档。

### 4. 为什么不直接加一把分布式锁？

锁可以控制并发，但不一定能补上订单归属语义；如果锁内仍只看 userId，错误逻辑仍可能执行。并且新锁会增加超时、续期和锁粒度等复杂度。这个项目先复用已有 orderId，是更小的直接修复。

### 5. 为什么 suspect 也要带 orderId？

suspect 记录首次发现时间。如果新旧订单共用一个 key，B 可能继承 A 的旧时间，提前满足补偿条件；旧订单清理时也可能删掉 B 的记录。key 追加 orderId 后，新旧订单分别计时和清理。

### 6. 为什么 orderId 要按字符串处理？

订单 ID 是很长的整数，转成 Lua 数值可能带来精度风险。这里只做身份比较，不做数值计算，所以字符串更合适。库存和 TTL 才按数值处理。

### 7. Set 改 Hash 能直接上线吗？

不能这样推断。同一个 key 不能混用 Set 和 Hash 命令，直接切换可能出现 `WRONGTYPE`。本次只在空的专用 Redis 中验证；已有环境需要先停止相关写入并清理或迁移，在线迁移没有实测。

### 8. 14 项回归通过，能说明秒杀已经没有竞态了吗？

不能。它们只覆盖本次构造的用例和专用环境。原失败用例修复后通过，证明这个具体问题得到改善；自然时序、在线迁移、集群、多实例和长期压测仍未覆盖。

## 十一、可用于简历的精简描述

> 参与秒杀订单补偿竞态排查与验证，在 AI 辅助下将 Redis 占坑由 Set 调整为保存 userId→orderId 的 Hash，在 Lua 中原子完成订单归属校验与幂等回滚，并隔离对账 suspect；通过确定性用例复现旧补偿误撤销新占坑问题，修复后相关 14 项回归通过。

## 十二、面试前必须能结合代码回答的 5 个问题

1. `rollbackSeckill.lua` 中 `currentOrderId` 和 `expectedOrderId` 分别从哪里来？如果不一致，库存和占坑应保持什么状态？
2. 为什么 `HGET`、归属比较、`SET NX`、`INCRBY`、`HDEL` 要放在同一个 Lua？拆到 Java 中会有什么具体时序问题？
3. `seckill.lua` 如何用 Hash 同时实现一人一单和订单归属记录？orderId 为什么不能用 Lua 数值？
4. `SeckillReconciliationTask.reconcileVoucher()` 中，数据库订单查询和 orderId 归属校验分别解决什么问题？为什么不能只保留其中一个？
5. suspect key 增加 orderId 后，A 和 B 的首次发现时间及清理过程有什么不同？Set 改 Hash 在已有数据环境还需要先处理什么？

如果以上问题不能脱离口述稿、直接结合代码回答，就应把面试表述收窄为“参与学习和验证”，不要声称已经独立掌握全部设计与实现。
