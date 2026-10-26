# 秒杀模块第二阶段基线测试报告

## 1. 基线与结论

- 日期：2026-10-26
- 基线 commit：`ed72f4cb7c4baac9bab89a2c9d4289bdb4bed533`（测试改动未提交）
- Run ID：`phase2_20261026_095646`
- 资源：专用 MySQL schema `hmdp_seckill_phase1_20261026_095646`、Redis DB 15、RabbitMQ vhost `/hmdp-seckill-phase1-20261026-095646`
- **最终业务用例结果：5 项，实际通过 4 项，业务断言失败 1 项，未执行 0 项。**
- 失败项为“旧请求补偿后重新抢购，另一旧路径补偿再次迟到执行”。未修改业务代码、未停止中间件、未清理共享数据。
- 前置第一阶段补强：仅重跑并发与重复消息 2 项，2/2 通过；清理前已有 30 秒超时的 ready/unacked 完成屏障。

## 2. 执行方式与命令

凭据由环境变量传入，不在报告中记录。所有 Maven 命令均使用相同专用 `SECKILL_TEST_*` 环境：

```bash
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillTransactionTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillRetryTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillReconciliationTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillCompensationTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillCompensationRaceTest test
```

事务和补偿测试曾因测试装配问题重跑，详见第 5 节；这些装配错误不计为业务通过。实际测试数据源均执行了 `SELECT @@SESSION.sql_mode`，采样值为：

```text
ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
```

## 3. 实际用例结果

| 用例 | 入口、真实组件与故障范围 | 预期 | 实际结果与证据 | 状态 |
|---|---|---|---|---|
| 订单 INSERT 后库存更新失败 | 直接 MQ，真实消费者、事务、DB/Redis；测试 MyBatis 拦截器在 INSERT `proceed()` 返回后计数并令 UPDATE 连续失败 | 每次事务回滚；实际尝试 3 次；死信身份匹配 | voucher=21，order=9100000000001；`insertReturned=3`、`updateAttempts=3`；最终 orders=0、DB stock=1、Redis stock=0、占坑仍在；DLQ body 的 order/user/voucher 均匹配并已仅 ACK 本消息 | 实际通过 |
| 临时故障消费重试 | 同上，前两次 UPDATE 失败，第三次放行真实 UPDATE | 第三次成功且只落一单，DLQ 为空 | voucher=22，order=9100000000002；实际 attempts=3，orders=1，DB/Redis stock=0/0，DLQ=0；日志有 3 次接收记录 | 实际通过 |
| 两阶段对账及重复补偿 | 手动调用真实 `reconcile()`，真实 DB/Redis/MQ 守卫；仅 mock Redisson/Bloom 启动器 | 首见只标记；超宽限期补偿一次；重复对账不多加库存 | voucher=23；首见标记嫌疑，61005ms 后补偿；最终 stock=1、occupied=false、orders=0；重复 rollback=false，重复对账后仍为 1 | 实际通过 |
| 生产发送失败补偿 | Service + 真实 Redis；mock `RabbitTemplate` 抛 `AmqpException`，不经过真实 MQ 消费 | 库存和占坑恢复；同 token/跨 token 重复补偿不多加库存 | voucher=25，order=652906759240811497；返回“下单失败，请重试”；stock=1、occupied=false；sameTokenRepeat=false、otherTokenRepeat=false | 实际通过 |
| 旧补偿与重新抢购竞态 | Service/rollback + 真实 Redis；mock MQ 生产者；按固定顺序直接调用，不靠随机并发 | 旧补偿不得撤销新请求库存和占坑 | 同 token 迟到重试被拦截；但另一旧路径 token 返回 true，stock 从 0 变为 1，occupied 从 true 变为 false，与预期相反 | **业务断言失败** |

### 3.1 失败竞态的确定顺序与证据

1. 旧请求预扣：voucher=26，user=7300000005，oldOrder=9200000000001。
2. 使用 `order:9200000000001` 首次补偿成功，stock=1、无占坑。
3. 同用户重新抢购成功，newOrder=652906875204928490，stock=0、占坑存在。
4. 同一旧 token 再次补偿：返回 false，stock=0、占坑仍存在，此部分符合预期。
5. 尚未使用的旧对账 token `recon:26:7300000005` 迟到补偿：实际返回 true，stock=1、占坑被移除。

```text
PHASE2_EVIDENCE case=stale-compensation-race ... firstCompensation=true sameTokenLate=false otherTokenLate=true stockAfterLate=1 occupiedAfterLate=false
[旧补偿不得撤销重新抢购后的新占坑]
Expecting: <true> to be equal to: <false> but was not.
Tests run: 1, Failures: 1, Errors: 0, Skipped: 0
```

**代码推断（不是额外执行结果）**：当前回滚 Lua 只检查“该用户当前是否占坑”和“本 token 是否用过”，没有校验占坑属于哪个订单或哪一代请求。因此，新请求重新占坑后，未使用过的旧路径 token 仍可能通过两道检查并执行 INCR/SREM。该静态解释与本次实际失败一致，但未据此做任何业务修复。

## 4. 关键通过证据

```text
PHASE2_EVIDENCE case=tx-rollback-dlq ... insertReturned=3 updateAttempts=3 orders=0 dbStock=1 redisStock=0 occupied=true dlqIdentity=true
PHASE2_EVIDENCE case=retry-success ... attempts=3 orders=1 dbStock=0 redisStock=0 dlq=0
PHASE2_EVIDENCE case=reconciliation-idempotent ... firstSuspect=true compensatedStock=1 repeatedRollback=false finalStock=1 occupied=false orders=0
PHASE2_EVIDENCE case=producer-compensation ... sameTokenRepeat=false otherTokenRepeat=false stock=1 occupied=false
```

## 5. 测试装配问题（与业务问题分开）

1. 初次事务测试使用 `@SpyBean` 代理 MyBatis mapper，因 final JDK Proxy 无法 spy，上下文未启动，业务断言未执行。改用测试专用 MyBatis `Executor.update` 拦截器。
2. 第一次死信读取丢失消息类型头并错误强转 `LinkedHashMap`，业务重试和 DLQ 已发生，但测试身份解析报错。改为 Jackson 直接读取 body；核对遗留消息 order=9100000000001、user=7300000001、voucher=20 后，仅删除该条本轮消息。
3. 初次生产补偿测试从基类取得的 `RabbitTemplate` 不是子类 mock，故障未注入、业务补偿未执行。改为在两个补偿测试类显式声明 mock 后重跑。

以上均保留原始日志；最终通过结论只来自修正装配后的实际执行。竞态失败则发生在业务断言中，未自动修复或放宽断言。

## 6. 隔离与清理核验

- 每组 `@BeforeEach` 实际要求专用订单队列和 DLQ 的 ready/unacked 都为 0。
- 清理前使用 30 秒超时完成等待；死信核对身份后仅 ACK 本轮券对应消息，未 purge 共享队列。
- 最终专用库中本轮测试券=0、订单=0。
- 最终专用订单队列、DLQ 及其他测试 vhost 队列 ready/unacked 均为 0，消费者为 0。
- Redis 按 15 个明确残留 key 执行 DEL，复核 DB 15 的 `DBSIZE=0`；未执行 FLUSHDB。
- 专用 schema 和 vhost 保留；未停止或重启任何中间件。

## 7. 未覆盖与结论边界

- `count` 前置查重分支仍未单独验证。
- Publisher confirm nack/return、Broker/应用崩溃、Redis 重启或 Cluster、多实例对账、死信重放与人工恢复、非空 DLQ 下的对账守卫、长期压测：未执行。
- 通过项仅证明本次专用环境和构造输入下的实际行为；一次测试不代表全部时序或生产级可靠。
- **补偿竞态不是“全部解决”：本次已用确定性用例实际证明跨旧路径 token 场景存在失败。**
