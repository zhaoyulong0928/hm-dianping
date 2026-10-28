# 黑马点评（改造版）

> 基于黑马程序员《Redis 实战篇》教程项目「黑马点评」进行二次开发。
> `master` 分支保留教程原版基线，改造内容在 `feature/refactor` 分支上进行。

## 项目简介

一个本地生活服务类应用的后端，提供商户查询、优惠券秒杀、探店笔记、好友关注、附近商铺等功能，重点实践 Redis 的各类应用场景。

## 技术栈

| 分类 | 技术 |
| --- | --- |
| 语言 / 框架 | Java 8、Spring Boot、MyBatis-Plus |
| 数据库 | MySQL 8 |
| 缓存 / 中间件 | Redis（String / Hash / Set / ZSet / GEO / Stream）、Redisson、RabbitMQ |
| 其他 | Nginx（静态资源 + 反向代理）、Lua 脚本 |

## 功能模块

- **登录认证**：验证码 + Redis Token + 双拦截器，实现无状态会话
- **商户查询缓存**：缓存穿透（参数校验 + 布隆过滤器）/ 击穿 / 雪崩的处理，Cache Aside 更新策略
- **优惠券秒杀**：Lua 脚本原子校验 + RabbitMQ 异步下单 + 按订单归属的幂等补偿与定时对账
- **探店笔记 / 好友关注**：点赞、关注、Feed 流
- **附近商铺**：基于 Redis GEO 的距离排序与分页

## 环境要求

- JDK 8+
- MySQL 8（库名 `hmdp`，建表与数据脚本见 `src/main/resources/db/hmdp.sql`）
- Redis 6+
- RabbitMQ 3.x（秒杀异步下单，默认 `localhost:5672`，账号密码 `guest/guest`）
- Nginx（前端静态资源，可选）

## 本地启动

1. 导入数据库：执行 `src/main/resources/db/hmdp.sql`
2. 按需修改 `src/main/resources/application.yaml` 中的 MySQL / Redis 连接信息
3. 启动 `HmDianPingApplication`（默认端口 `8081`）
4. 前端：Nginx 监听 `8080`，`/api/` 反向代理到 `8081`

## 分支说明

| 分支 / 标签 | 说明 |
| --- | --- |
| `master` | 教程原版基线 |
| `v1-course-original` | 原版快照标签，可随时对比 / 回滚 |
| `feature/refactor` | 改造分支（当前开发分支） |

对比改造内容：

```bash
git diff master feature/refactor
```

## 改造计划

> 随着改造推进持续更新。

- [x] 商户缓存更新改为「事务提交后删缓存 + 延迟双删」
- [x] 延迟删除线程池支持「闲时回收 + 优雅关闭」
- [x] 修复秒杀订单消息 ACK 的 stream 名称错误（`s1` → `stream.orders`）
- [x] 秒杀开始/结束时间校验下沉到 Redis Lua（避免秒杀请求查数据库）
- [x] 秒杀异步下单由 Redis Stream 迁移到 RabbitMQ（可靠投递 + 并发消费 + 死信队列 + 唯一索引幂等）
- [x] 缓存重建锁改用 Redisson `RLock`（修复自研锁误删他人锁的隐患）
- [x] 互斥锁方案改为「有界重试 + 降级」（消除无限递归重试，避免请求线程被长时间占用）
- [x] 商户缓存读路径切换为逻辑过期方案：启动预热全量商铺 + 缓存 miss 查库自愈回填
- [x] 商户查询增加缓存穿透主动防线：参数校验 + 布隆过滤器（Redisson RBloomFilter，启动全量加载）
- [x] 延迟双删第二删升级为 RabbitMQ 延迟队列（TTL + 死信）可靠执行：消息持久化、宕机不丢、失败重试后进死信队列告警/人工补偿
- [x] 缓存延迟双删第二次删除改为「MQ 可靠路径 + 本地兜底」双保险：发送侧补 `CorrelationData`，并用本地延迟删除兜住「消息到达 broker 之前失败（同步异常/nack/路由失败）」的盲区
- [x] 登录安全加固：验证码一次性消费 + 发送频控 + 失败锁定（限用 / 限发 / 限猜）
- [x] 秒杀用户级限流：Lua 前置固定窗口（每用户每秒 1 次），业务校验前拦截脚本并行请求
- [x] 秒杀消费端幂等收敛：删除冗余 Redisson 锁，改由「唯一索引 uk_user_voucher + count 前置判重」兜底重复消息（Lua 原子占坑已从源头拦截同用户重复请求）
- [x] 秒杀订单落库顺序修正：订单插入前置于库存扣减，封死「扣库存无订单」脏提交窗口
- [x] 秒杀消费端终态失败（库存不足）就地补偿：手动标记事务回滚 + 回滚 Redis 后正常 ack，死信队列退化为「仅承接基础设施故障、有消息即告警」的健康仪表盘
- [x] 秒杀回滚幂等与订单归属校验：占坑改为 Hash（userId → 字符串 orderId），`rollbackSeckill` 在单个 Lua 中完成归属比较 + SETNX 防重 + 回滚，避免旧补偿误撤销新订单占坑
- [x] 秒杀定时对账（anti-entropy）：兜底 confirm 丢失/进程蒸发等静默漂移，两阶段裁决防在途误伤；suspect 按订单隔离，实弹修复 7 天前 voucher-11 历史漂移
- [x] 秒杀消费端占坑双检：落库前后 HEXISTS 校验，封死「对账误补偿 × 在途落库」竞态，杜绝幽灵订单与库存基线漂移
- [x] 秒杀并发一致性测试：防超卖（100 库存 / 1000 并发）+ 重复消息幂等（同一(用户,券) 5 条消息只落 1 单）（`SeckillConcurrencyTest`）
- [x] 秒杀两阶段基线测试：真实 HTTP 全链路、时间/库存边界、重复抢购、并发防超卖、重复消息、事务回滚、消费重试/死信、补偿与对账
- [x] 秒杀旧补偿竞态修复回归：原失败用例保持断言不变并转为通过，两阶段受影响用例 + 新增 suspect 隔离共 14 项针对性回归通过
- [x] 秒杀活动 Redis 初始化一致性：数据库提交后经 `afterCommit` 发布（时间键先写、库存最后写），Lua 新增「活动未就绪」返回码 6；对账仅补静态时间键，库存缺失/不一致只告警；首次初始化失败、回调前崩溃、进行中库存丢失保留人工恢复边界

## 改造记录

> 维护约定：每次推送到远程前，同步更新「改造计划」勾选项与本节记录，保持 README 与实际代码一致。

| 日期 | 内容 |
| --- | --- |
| 2026-10-03 | 商户 `update` 改为「事务提交后删缓存 + 延迟双删」（`ShopServiceImpl`） |
| 2026-10-04 | 延迟删除线程池增加「闲时回收（`allowCoreThreadTimeOut`）+ 优雅关闭（`@PreDestroy`）」（`ShopServiceImpl`） |
| 2026-10-05 | 修复秒杀订单消息 ACK 的 stream 名称错误（`s1` → `stream.orders`），避免 pending 消息堆积（`VoucherOrderServiceImpl`） |
| 2026-10-09 | 秒杀增加开始/结束时间校验：起止时间写入 Redis，由 `seckill.lua` 判断；并修复库存 key 不存在时 `tonumber(nil)` 报错的容错（`VoucherServiceImpl` / `seckill.lua` / `VoucherOrderServiceImpl` / `RedisConstants`） |
| 2026-10-10 | 秒杀异步下单由 Redis Stream 迁移到 RabbitMQ：publisher confirm + 消息持久化 + 失败重试 3 次进死信 + 并发消费（`concurrency`）+ `(user_id, voucher_id)` 唯一索引幂等兜底（新增 `RabbitMQConfig` / `MqConstants` / `SeckillOrderMsg`，改造 `VoucherOrderServiceImpl` / `seckill.lua` / `application.yaml` / `hmdp.sql`） |
| 2026-10-13 | 缓存重建锁改用 Redisson `RLock`，修复原自研锁「释放不校验持有者、极端场景误删他人锁」的隐患（避免缓存击穿时多个线程同时打库）；逻辑过期方案将「拿锁 + 重建 + 释放锁」收敛到同一异步线程，规避 Redisson 锁跨线程释放报错（`CacheClient`） |
| 2026-10-13 | 互斥锁方案消除无限递归重试：改为「抢锁失败等待 30~50ms 重查缓存，最多重试 3 次，仍失败抛 `CacheBusyException` 降级」；抢锁成功后先 double-check 缓存避免重复查库；中断时恢复标记并快速降级（`CacheClient` / `RedisConstants`，新增 `CacheBusyException` 及全局降级响应） |
| 2026-10-13 | 商户缓存读路径切换为逻辑过期方案：新增 `ShopCachePreloader` 启动预热（`ApplicationRunner` 全量商铺写入逻辑过期缓存）；修复「延迟双删物理删除 key 后，逻辑过期 miss 直接返回 null 误报店铺不存在」的缺陷——miss 时查库自愈并以逻辑过期格式回填（回填加锁防并发）；`ShopServiceImpl.queryById` 由穿透方案切换为逻辑过期方案 |
| 2026-10-13 | 商户查询增加缓存穿透主动防线：`queryById` 前置「参数校验 + 布隆过滤器」两道拦截，"从未存在过"的 id 不再查缓存/查库/写空值 key；布隆基于 Redisson `RBloomFilter`（预期 10 万元素 / 1% 误判率，位图约 115KB），启动时全量加载店铺 id，`add` 幂等可自愈；布隆误判与已删除店铺由读路径查库兜底（新增 `ShopBloomFilter`，改造 `ShopServiceImpl` / `RedisConstants`） |
| 2026-10-14 | 延迟双删第二删升级为 RabbitMQ 延迟队列可靠执行：移除本地 `ScheduledThreadPoolExecutor` 定时删除（一次性执行、宕机即丢），改为事务提交后发送 `CacheDeleteMsg` 到延迟队列（`x-message-ttl=500ms` + 死信投递），由 `CacheDeleteListener` 消费执行二次删除；消息持久化在 RabbitMQ，进程宕机不丢；删除失败走全局重试拦截器（3 次）后进入 `cache.dlx.queue` 供告警/人工补偿；删除 key 幂等，可安全重试（新增 `CacheDeleteMsg` / `CacheDeleteListener`，改造 `ShopServiceImpl` / `RabbitMQConfig` / `MqConstants`） |
| 2026-10-14 | 修复逻辑过期方案「数据删除」边界缺陷：异步重建时若数据库已无数据（`dbFallback` 返回 null），直接删除缓存 key 而非写入 `data=null` 的 `RedisData`——Jackson `non_null` 会丢弃 `data` 字段，下次读取时 `(JSONObject) null` 反序列化抛 `ClassCastException`，且新 `expireTime` 在未来导致异常被固化在缓存中；删除 key 后由 miss 自愈路径重新查库返回「店铺不存在」，行为一致（`CacheClient`） |
| 2026-10-15 | 缓存延迟双删第二次删除改为「MQ 可靠路径 + 本地兜底」双保险：在原有 MQ 延迟队列的基础上，恢复本地 `ScheduledThreadPoolExecutor` 兜底删除（闲时回收 + `@PreDestroy` 优雅关闭），两条路径互为盲区补位——本地兜住「消息到达 broker 之前失败（同步异常 / broker nack / 路由失败）」，MQ 兜住「本地任务执行前进程宕机」；删除 key 幂等，重复执行无副作用。同时发送侧补 `CorrelationData(key)`，便于 confirm 回调定位失败消息（`ShopServiceImpl`） |
| 2026-10-15 | 登录安全加固（秒杀防脚本的前提层，对应验证码攻击面「限发 / 限猜 / 限用」三环节）：① 验证码校验通过后立即删除（一次性消费，关闭 2 分钟重放窗口，一个 code 不能再换取多个 token）；② `sendCode` 增加发送频控——复用验证码 2 分钟 TTL 作为天然冷却期，未过期拒绝重发（防短信轰炸/刷接口）；③ 登录失败计数：连续失败 5 次锁定 120 秒（与验证码有效期对齐），防 6 位验证码暴力枚举（无锁定时 10⁶ 组合约 8300 QPS 即可在有效期内穷举）（`UserServiceImpl`） |
| 2026-10-16 | 秒杀用户级限流：`seckill.lua` 最前置固定窗口限流（`INCR`+`EXPIRE`，每用户每秒最多放行 1 次，返回码 5），在时间/库存/一人一单校验之前拦截并行/高频的脚本请求，不产生任何业务写操作；`VoucherOrderServiceImpl` 增加返回码 5 的降级响应（`seckill.lua` / `VoucherOrderServiceImpl`；另含 lua 文件 EOL 归一化 CRLF→LF 及 Redis Cluster 适用性注释） |
| 2026-10-16 | 秒杀订单落库顺序修正：订单插入前置于库存扣减——并发重复消息在扣库存之前即被唯一索引拦截，即使 `DuplicateKeyException` 被外层 catch 吞掉、事务照常提交，事务内也未发生任何写操作，封死「扣库存无订单」的脏提交窗口（解锁早于事务提交 + MVCC 快照读看不见未提交订单，重复消息可绕过查重直达 save）；库存不足时抛异常回滚含订单的整个事务，重试耗尽进死信队列（`VoucherOrderServiceImpl`） |
| 2026-10-16 | 秒杀消费端区分「终态失败 vs 瞬时故障」：库存不足（Redis/DB 账本漂移所致，lua 前置校验后概率≈0，重试无意义）不再走「重试 3 次 → 死信」路径，改为 catch `IllegalStateException` 就地了结——`TransactionAspectSupport.setRollbackOnly()` 手动标记回滚（防 catch 吞异常后 save 的 INSERT 随事务提交成「有订单没扣库存」脏数据）+ `rollbackSeckill` 补偿回滚 Redis 库存与一人一单记录后正常 ack；补偿自身失败仍自然回落到重试/死信路径（Redis 故障期间自动降级为告警模式）；死信队列因此退化为「仅承接基础设施故障（DB/Redis 抖动超重试窗口、毒消息）、有消息即告警」的健康仪表盘，历史测试死信已清除归零（`VoucherOrderServiceImpl`） |
| 2026-10-16 | 秒杀回滚幂等化：`rollbackSeckill` 由两条独立命令（INCRBY + SREM）改为 `rollbackSeckill.lua` 单脚本原子执行，消除「INCRBY 成功后 SREM 失败、重试再次 INCRBY」的库存虚增隐患；并以 orderId 粒度 `SET NX EX`（TTL 24h）抢占回滚权——同一 rollbackToken 无论重试多少次都只生效一次；返回是否实际回滚，重复触发记 warn 日志便于观测补偿路径异常。不同 token 只能区分补偿来源，不能证明当前占坑仍属于旧订单，后续于 2026-10-26 补充订单归属校验（新增 `rollbackSeckill.lua`，改造 `VoucherOrderServiceImpl` / `RedisConstants`；已用 redis-cli 实测首次回滚/重复回滚/新订单回滚三种时序） |
| 2026-10-17 | 秒杀消费端占坑双检：封死「对账误补偿 × 在途落库」竞态——极端在途（DB 行锁等待 50s×3 次 ≈ 153s）可超过对账 60s 宽限期，且 `getQueueInfo().getMessageCount()`（AMQP passive declare 语义）不含已投递未 ack 消息导致守卫放行，误补偿先执行、卡顿消费者醒来后落库，会制造「有单无占坑 + Redis 基线 +1」的幽灵库存：秒杀尾部沦为「lua 过 → DB 拒 → 就地补偿」的假成功死循环，而对账单向扫描（占坑集合→DB 订单）对该反向漂移永久不可见。修复：`createVoucherOrder` 落库前资格预检（占坑已被补偿回滚则跳过落库直接 ack）+ `onOrderMessage` 落库成功后、事务提交前终检（消费期间被补偿则 `setRollbackOnly` 撤销订单与扣库存、正常 ack——资格已退还用户可重新下单，重试无意义）；两检合围补偿可能发生的全部时序（消费前/消费中），正常路径零影响（合法消息占坑恒在），终检与终态失败路径同用 `setRollbackOnly` 模式；对账任务设计注释同步修正「在途上限 ≈ 10s」的不实估算，改为记录分层防御关系（`VoucherOrderServiceImpl` / `SeckillReconciliationTask`，另含对账任务与回滚脚本的学习注释补充） |
| 2026-10-24 | 秒杀消费端删除冗余 Redisson 分布式锁：Lua 的 `SISMEMBER+SADD` 已原子拦截同(用户,券)重复请求，落库重复由 `uk_user_voucher` 唯一索引抛 `DuplicateKeyException` 兜底；原「抢锁失败即 return」分支会在持锁消息重试耗尽进死信时静默吞消息，故移除。保留 `count` 前置判重以减少无效 INSERT；新增 `SeckillConcurrencyTest`（100 库存/1000 并发防超卖 + 5 条重复消息只落 1 单）（`VoucherOrderServiceImpl` / `SeckillConcurrencyTest`） |
| 2026-10-26 | 秒杀补偿增加订单归属校验：第二阶段确定性测试复现「旧请求预扣 → 首次补偿 → 同用户重新抢购 → 另一旧路径 token 迟到」导致 Redis 库存多恢复 1、新占坑被误删；根因是 Set 占坑只保存 userId，而 token 只能做补偿来源幂等，不能证明当前占坑归属。最小修复为占坑改 Hash（field=userId、value=字符串 orderId），`rollbackSeckill` 增加 `expectedOrderId`，并在同一个 `rollbackSeckill.lua` 中完成 `HGET` 字符串比对、`SET NX EX`、`INCRBY + HDEL`；对账 suspect key 追加 orderId，独立记录新旧订单宽限期和清理范围，保留 DB 是否已成功落单的判断。原失败用例未改弱断言，修复后返回 false、stock=0、占坑保留；第一阶段 8 项 + 第二阶段原有 5 项 + 新增 suspect 隔离 1 项，共 14 项针对性回归实际通过（`seckill.lua` / `rollbackSeckill.lua` / `IVoucherOrderService` / `VoucherOrderServiceImpl` / `SeckillReconciliationTask` / `RedisConstants`；报告见 `seckill-phase1-baseline-report.md` / `seckill-phase2-baseline-report.md` / `seckill-compensation-fix-report.md`，面试复盘见 `seckill-stale-compensation-interview.md`）。仅在空的专用 Redis 完成类型切换，未验证已有 Set/旧 suspect 在线迁移，不宣称解决全部竞态或达到生产级可靠性 |
| 2026-10-28 | 秒杀活动 Redis 初始化一致性：`addSeckillVoucher` 改为数据库提交后通过 `TransactionSynchronization#afterCommit` 发布 Redis 状态，时间键先写、库存键最后写且全部 `setIfAbsent`；Controller 与 Service 双重校验起止时间/库存，事务前提在写库前快速失败。新增 `SeckillActivityRedisInitializer` 封装首次发布与静态补齐，异常统一在公共入口捕获并返回 `PublishResult`；`seckill.lua` 增加必要 key 就绪检查（缺 key/非法值返回 6，`stock=0` 仍返回 1），限流保持在就绪检查之后；对账在守卫前仅补静态时间键，库存缺失/不一致只告警，不自动重灌。新增 `SeckillActivityRedisInitializerTest` / `SeckillReconciliationTaskIsolationTest` / `SeckillActivityInitIntegrationTest` / `SeckillActivityInitFailureTest`。独立环境验收：单测 9 项、新增集成 10 项、既有回归 8 项、边界 5 项、HTTP 1 项、调度隔离 1 项、Lua 16 项断言全部通过；验收报告与日志在本地留存（未纳入仓库）。边界：首次初始化失败不自动恢复可售，`afterCommit` 前进程崩溃仍可能缺状态，进行中库存丢失需人工受控恢复，未引入 outbox/任务表/延迟重试 |
| 2026-10-28 | 补充 RabbitMQ publisher confirm/return 与重试触发条件注释，明确路由失败缺口由对账兜底；移除 `VoucherOrderController` 作者标记。仅注释/文档变更，无功能改动（`RabbitMQConfig` / `VoucherOrderController`） |