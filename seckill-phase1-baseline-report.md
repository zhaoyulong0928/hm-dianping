# 秒杀模块第一阶段基线测试报告

## 1. 基线与范围

- 日期：2026-10-26
- Commit：`ed72f4cb7c4baac9bab89a2c9d4289bdb4bed533`
- Run ID：`phase1_20261026_095646`
- 环境：JDK 17.0.20.1、Maven 3.9.11、MySQL 8.0.46、Redis、RabbitMQ 4.0
- 本阶段只覆盖 HTTP 正常全链路、时间/库存边界、重复抢购、并发防超卖、重复消息幂等。
- 未修改业务代码，未停止中间件，未清理共享数据。新增/修改范围仅为测试代码、测试配置和本报告。

## 2. 隔离与清理

- MySQL：新建专用 schema `hmdp_seckill_phase1_20261026_095646`，仅在该 schema 初始化表结构和基础数据；实测存在唯一索引 `uk_user_voucher`。
- Redis：使用 DB 15，执行前 `DBSIZE=0`；测试中的 StringRedisTemplate 均指向 DB 15，未使用 `FLUSHDB`。
- RabbitMQ：新建专用 vhost `/hmdp-seckill-phase1-20261026-095646`，未修改其他 vhost 或共享队列。
- 测试后：本轮测试券和订单计数均为 0；Redis 按 15 个明确残留 key 执行 `DEL`，复核 `DBSIZE=0`。
- 专用队列复核：`seckill.order.queue`、`seckill.dlx.queue` 等 ready/unacked 均为 0，消费者为 0。
- 专用 schema 和 vhost 保留，等待第二阶段确认；未删除其中任何共享资源。

## 3. 实际执行命令

凭据通过环境变量传入，以下省略密码值。

```bash
/tmp/apache-maven-3.9.11/bin/mvn -DskipTests test-compile

SECKILL_TEST_RUN_ID=phase1_20261026_095646 \
SECKILL_TEST_DB_URL='jdbc:mysql://127.0.0.1:3306/hmdp_seckill_phase1_20261026_095646?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true' \
SECKILL_TEST_REDIS_HOST=127.0.0.1 SECKILL_TEST_REDIS_DB=15 \
SECKILL_TEST_RABBIT_HOST=127.0.0.1 \
SECKILL_TEST_RABBIT_VHOST=/hmdp-seckill-phase1-20261026-095646 \
/tmp/apache-maven-3.9.11/bin/mvn \
  -Dtest=SeckillHttpTest,SeckillBoundaryTest,SeckillConcurrencyTest test
```

实际结果汇总：**最终执行 8 个，实际通过 8 个，失败 0，错误 0，跳过 0**。首次执行因测试装配错误 8 个均未进入业务断言，详见“问题记录”，不计为业务测试失败或通过。

## 4. 用例结果

| 用例 | 入口及 mock 范围 | 预期 | 实际关键结果 | 状态 |
|---|---|---|---|---|
| HTTP 正常全链路 | 随机端口真实 POST；真实登录拦截、Lua、MQ、DB；仅 mock 对账、Redisson 和商铺 Bloom 启动器 | 返回订单 ID，落一单，双边库存归零 | HTTP 200；voucher=17，user=7100000001，order=652895682520155120；orders=1，DB/Redis stock=0/0 | 实际通过 |
| 未开始 | Service + 真实 Lua/Redis；mock MQ 生产者 | 拒绝且不写库存/占坑 | voucher=13；`秒杀尚未开始`，stock=2，occupied=false | 实际通过 |
| 已结束 | 同上 | 拒绝且不写库存/占坑 | voucher=14；`秒杀已经结束`，stock=2，occupied=false | 实际通过 |
| 零库存 | 同上 | 拒绝且不写占坑 | voucher=15；`库存不足`，stock=0，occupied=false | 实际通过 |
| 最后一份库存 | 同上 | 仅第一次放行，第二次库存不足 | voucher=12；firstSuccess=true，secondError=`库存不足`，stock=0 | 实际通过 |
| 重复抢购 | 同上；间隔 1.1 秒避开限流 | 第二次拒绝，不重复扣减 | voucher=16；firstSuccess=true，secondError=`不能重复下单`，stock=1 | 实际通过 |
| 并发防超卖 | 复用现有 Service 集成测试；真实 Redis/MQ/DB；mock 对账等无关组件 | 100 库存、1000 用户不超卖且账本一致 | voucher=10；success=100，orders=100，DB/Redis stock=0/0 | 实际通过 |
| 重复消息幂等 | 复用现有测试，直接发 MQ，绕过 HTTP/Lua；真实消费者和 DB | 5 条同用户同券消息只落一单、只扣一次 | voucher=11；orders=1，DB/Redis stock=9/9；消息 ID 9000000000000～4，另 4 条记录重复忽略日志 | 实际通过 |

关键日志：

```text
PHASE1_EVIDENCE case=http-full ... orderId=652895682520155120 http=200 orders=1 dbStock=0 redisStock=0
PHASE1_EVIDENCE case=concurrency voucherId=10 users=1000 success=100 orders=100 dbStock=0 redisStock=0
重复下单消息，忽略: SeckillOrderMsg(id=9000000000004, userId=2000000, voucherId=11)
PHASE1_EVIDENCE case=duplicate-message voucherId=11 ... sent=5 orders=1 dbStock=9 redisStock=9
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

### 4.1 基于既有源码与日志的补充核验

1. **唯一键冲突分支已实际触发，count 前置查重分支本次未验证。** 5 条重复消息均有“收到秒杀下单消息”日志，其中 4 条随后记录“重复下单消息，忽略”；该日志位于 `DuplicateKeyException` 捕获分支。未出现“用户已购买过，忽略”日志。因此本次是 1 条成功、4 条唯一键冲突，并非全部由 count 查重返回。
2. **原测试缺少显式完成屏障，现已补强并重跑受影响用例。** 原并发用例仅等待订单数达到 100，原重复消息用例仅等待订单数达到 1；虽然原日志支持消息先处理后清理，但源码保障不足。现已在订单数等待后增加 30 秒超时的 Management API 屏障，要求专用订单队列 ready/unacked 都为 0，再读取最终订单与库存并清理。仅重跑这两个用例，结果 2/2 通过：并发 voucher=18、orders=100、DB/Redis stock=0/0；重复消息 voucher=19、orders=1、DB/Redis stock=9/9。
3. **SQL mode 不同，应用 session 已直接采样。** 初始化脚本连接通过 `--init-command="SET SESSION sql_mode=''"` 使用空 SQL mode。补强后的测试通过实际 `DataSource` 执行 `SELECT @@SESSION.sql_mode`，两次均输出 `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`，与 JDBC 未显式设置时继承服务器 GLOBAL 的配置一致。

## 5. 问题记录

1. **专用 schema 初始化问题（已处理）**：MySQL 8 会话 SQL mode 拒绝脚本中的零日期默认值。仅在专用 schema 的初始化连接设置会话级 `sql_mode=''` 后成功；未修改全局配置或业务代码。
2. **首次测试装配错误（已处理）**：mock Redisson 后，无关启动器 `ShopBloomFilter.run()` 因空 BloomFilter 触发 NPE，首次结果为 `Tests run: 8, Errors: 8`，业务断言未开始。随后在三个测试类同时 mock 该启动器并重跑，才有上述 8/8 通过结果。
3. 本阶段未发现实际业务断言失败。一次并发和一次重复消息执行不能证明所有时序、全链路或生产级可靠性。

## 6. 未执行与未覆盖

- 第二阶段事务回滚、消息异常、消费重试/死信、Redis 补偿、对账幂等：**未执行**。后续重试必须实际计数，死信必须核对消息身份；事务回滚必须先确认订单 INSERT 已执行再注入失败。
- **“旧请求补偿后用户重新抢购，旧补偿再次迟到执行”补偿竞态：未覆盖。** 不宣称补偿竞态已全部解决。
- Publisher confirm nack/return、Broker/应用进程崩溃、Redis 重启或 Cluster、多实例对账、长时间压测、死信重放和人工恢复：未覆盖。
