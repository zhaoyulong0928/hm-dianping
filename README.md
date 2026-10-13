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
- **商户查询缓存**：缓存穿透 / 击穿 / 雪崩的处理，Cache Aside 更新策略
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