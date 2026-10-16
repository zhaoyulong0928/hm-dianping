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
- **优惠券秒杀**：Lua 脚本原子校验 + 分布式锁 + RabbitMQ 消息队列异步下单
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

## 改造记录

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