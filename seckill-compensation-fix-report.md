# 秒杀旧补偿竞态最小修复与回归报告

## 1. 结论

- 基线 commit：`ed72f4cb7c4baac9bab89a2c9d4289bdb4bed533`，修改未提交。
- Run ID：`fix_20261026_1416`。
- 按最小方案完成 Hash 占坑、字符串 orderId 归属校验、suspect 按订单隔离；未引入新表、分布式锁或其他架构改造。
- **最终 14 个不同回归用例全部实际通过，业务失败 0，未执行 0。**
- 原竞态用例的业务预期和断言未修改；修复前它为业务失败，修复后实际通过。
- 未停止中间件，未修改或清理共享数据。

## 2. 修改文件清单

### 业务代码

| 文件 | 最小修改 |
|---|---|
| `src/main/resources/seckill.lua` | 占坑 Set 改 Hash：`HEXISTS` 查重、`HSET userId orderId`；orderId 直接使用 ARGV 字符串，不转数值 |
| `src/main/resources/rollbackSeckill.lua` | `HGET` 当前订单并与字符串 `expectedOrderId` 比较；匹配才允许 `INCRBY + HDEL`；保留 token 幂等闸门 |
| `IVoucherOrderService.java` | `rollbackSeckill` 增加 `String expectedOrderId` 参数 |
| `VoucherOrderServiceImpl.java` | 生产/消费补偿传入订单 ID 字符串；执行归属校验；占坑查询改 `HEXISTS`；DB 事务和原有业务判断保留 |
| `SeckillReconciliationTask.java` | 扫描占坑 Hash，保留 DB 是否已落单查询；补偿传入当前订单 ID；suspect key 追加 orderId |
| `RedisConstants.java` | 更新 suspect key 格式注释 |

### 测试代码

- `SeckillConcurrencyTest.java`：重复消息预扣数据改为 Hash。
- `SeckillIntegrationTestSupport.java`：预扣和占坑断言改 Hash；组前队列检查增加 30 秒超时完成等待。
- `SeckillBoundaryTest.java`：占坑查询改 Hash。
- `SeckillTransactionTest.java`、`SeckillRetryTest.java`：预扣时保存订单 ID 字符串。
- `SeckillCompensationTest.java`、`SeckillCompensationRaceTest.java`：补偿传入期望订单 ID；原竞态断言不变。
- `SeckillReconciliationTest.java`：适配订单归属参数和 suspect key；新增旧/新订单 suspect 隔离用例。

## 3. 全局调用点核验

- 秒杀占坑路径已无 `SADD/SISMEMBER/SREM/SMEMBERS` 或对应 Java Set 调用；剩余 `opsForSet` 仅在无关的关注业务。
- `seckill.lua` 的 `orderId=ARGV[3]` 直接写入 Hash；回滚 Lua 的 `ARGV[2]` 直接进行字符串比较。仅 TTL 使用 `tonumber(ARGV[3])`。
- 生产失败、消费终态、对账及全部测试补偿调用均已传入 `expectedOrderId`。
- suspect 只在当前订单 key 上标记和清理：`{vid}:{uid}:{orderId}`。旧订单 key 不覆盖、不删除新订单 key，旧 key 由 TTL 清理。
- 对账仍先查询 DB 判断订单是否已成功；orderId 归属校验只限制补偿对象，没有替代该判断。

## 4. 类型切换与执行命令

切换前专用 Redis DB 15 为空，订单队列和 DLQ ready/unacked 均为 0、消费者为 0，专用订单表为 0。因此没有旧 Set 数据需要迁移，直接在本轮专用空资源中初始化 Hash 数据。

```bash
/tmp/apache-maven-3.9.11/bin/mvn clean test-compile

# 以下命令均使用相同专用 SECKILL_TEST_* 环境
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillCompensationRaceTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillCompensationTest,SeckillReconciliationTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillTransactionTest,SeckillRetryTest,SeckillConcurrencyTest,SeckillBoundaryTest,SeckillHttpTest test
/tmp/apache-maven-3.9.11/bin/mvn -Dtest=SeckillTransactionTest test
```

完整编译实际编译 80 个主代码源文件和 15 个测试源文件，成功。

## 5. 实际回归结果

| 范围 | 实际结果 | 关键证据 |
|---|---|---|
| 原旧补偿竞态 | 1/1 通过 | old=9200000000001，new=652959750547308545；sameTokenLate=false、otherTokenLate=false、stock=0、occupied=true |
| 生产失败补偿 | 1/1 通过 | stock=1、无占坑；同 token/跨 token 重复补偿均 false |
| 对账幂等 | 1/1 通过 | 首见 suspect、到期补偿一次；重复 rollback=false，最终 stock=1 |
| 新旧订单 suspect 隔离（新增） | 1/1 通过 | lateOldCompensation=false；oldSuspectKept=true、newSuspectDeleted=true；新订单宽限期独立 |
| 事务回滚及死信 | 1/1 通过 | insertReturned=3、updateAttempts=3、orders=0、DB stock=1、DLQ 身份匹配 |
| 消费重试 | 1/1 通过 | attempts=3、orders=1、DB/Redis stock=0/0、DLQ=0 |
| 并发与重复消息 | 2/2 通过 | 1000 用户/100 库存：orders=100、stock=0/0；5 条重复消息：orders=1、stock=9/9 |
| 时间与库存边界、重复抢购 | 5/5 通过 | 未开始、已结束、零库存、最后库存、重复抢购均满足原断言 |
| 真实 HTTP 全链路 | 1/1 通过 | HTTP 200，order=652960098439660530，orders=1，DB/Redis stock=0/0 |

组合回归曾有一次组前检查读到前一测试类退出过程中的 `unacked=1`，事务业务方法未执行。将组前检查也改为 30 秒超时等待后，事务用例单独重跑通过。这是测试上下文切换等待问题，未修改业务断言。

## 6. 清理与未执行项

- 最终专用测试券和订单均为 0；专用订单队列、DLQ ready/unacked 均为 0。
- Redis 按 15 个明确残留 key 执行 DEL，复核 DB 15 为 0；未 FLUSHDB，未清理共享队列。

未执行及剩余边界：

- 只在空的专用 Redis 中完成类型切换；未执行已有 Set 数据的在线迁移，也未修改共享 Redis。已有环境部署仍需先在静止窗口处理类型兼容性。
- 未验证旧格式 suspect 的在线迁移；新逻辑只使用带 orderId 的 suspect key。
- `count` 前置查重分支仍未单独验证。
- Publisher confirm nack/return、进程/Broker 崩溃、Redis Cluster、多实例对账、长期压测和死信重放未执行。
- 本次仅修复已验证的旧补偿误撤销新占坑问题，不宣称其他消费/对账竞态或生产级可靠性已全部解决。
