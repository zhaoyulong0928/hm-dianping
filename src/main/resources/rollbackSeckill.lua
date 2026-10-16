-- 幂等回滚秒杀预扣（库存 + 一人一单记录），修复原 rollbackSeckill 两个缺陷：
--   1. 非原子：INCRBY 与 SREM 原为两条独立命令，INCRBY 成功后 SREM 失败，
--      重试会再次 INCRBY 导致库存虚增（超发隐患）
--   2. 非幂等：同一订单的补偿可能被多条失败路径（生产端同步异常 / 消费端终态补偿 /
--      未来的 confirm nack / return / 死信消费者）重复触发，库存被多次 +1
-- 方案：以 orderId 为粒度 SET NX 抢占回滚权（orderId 全局唯一），同一订单无论重试
--      多少次、被几条路径触发，回滚只生效一次；标记带 TTL 自动清理
-- 注意：本脚本操作 orderId / voucherId 派生的多种 key，未使用 hash tag，
--      仅适用单机 Redis（与 seckill.lua 的适用性约定一致）

-- KEYS[1] 回滚标记 key（seckill:rollback:{orderId}）
-- KEYS[2] 库存 key（seckill:stock:{voucherId}）
-- KEYS[3] 一人一单集合 key（seckill:order:{voucherId}）
-- ARGV[1] 用户 id
-- ARGV[2] 标记 TTL（秒）

-- 1.幂等闸门：抢占本订单的回滚权（SET NX EX 原子，EX 保证标记最终过期清理）
--   抢占失败 = 该订单已回滚过（重复补偿 / 并发补偿），直接跳过返回 0
if not redis.call('set', KEYS[1], '1', 'EX', tonumber(ARGV[2]), 'NX') then
    return 0
end

-- 2.回滚库存：+1 抵消 seckill.lua 对应订单的 INCRBY -1
redis.call('incrby', KEYS[2], 1)

-- 3.回滚一人一单记录：移除用户，放行其重新下单
--   若用户已被同用户其他订单的回滚先行移除（先补偿旧单、用户重新下单、再补偿新单的
--   时序），SREM 返回 0 属正常：每个订单的扣减与回滚按标记一比一配对，无条件执行即维持账目平衡
redis.call('srem', KEYS[3], ARGV[1])

-- 4.回滚成功返回 1
return 1
