-- 幂等回滚秒杀预扣（库存 + 一人一单记录），修复原 rollbackSeckill 两个缺陷：
--   1. 非原子：INCRBY 与 SREM 原为两条独立命令，INCRBY 成功后 SREM 失败，
--      重试会再次 INCRBY 导致库存虚增（超发隐患）
--   2. 非幂等：同一订单的补偿可能被多条失败路径（生产端同步异常 / 消费端终态补偿 /
--      未来的 confirm nack、return、死信消费者、定时对账）重复触发
-- 三重防线按序执行，任一不满足直接跳过（返回 0）：
--   1. SISMEMBER 业务守卫：用户已不占坑 = 预扣资格已被其他路径释放（不同 token 的
--      补偿路径，如死信补偿 vs 对账补偿），无可回滚之物 —— 防跨路径双重回滚虚增库存
--   2. SET NX EX 幂等闸门：同一 rollbackToken 只能回滚一次 —— 防同一路径重试
--   3. INCRBY + SREM：回滚动作
-- 注意：本脚本操作 rollbackToken / voucherId 派生的多种 key，未使用 hash tag，
--      仅适用单机 Redis（与 seckill.lua 的适用性约定一致）

-- KEYS[1] 回滚标记 key（seckill:rollback:{rollbackToken}，token 如 order:{orderId} / recon:{vid}:{uid}）
-- KEYS[2] 库存 key（seckill:stock:{voucherId}）
-- KEYS[3] 一人一单集合 key（seckill:order:{voucherId}）
-- ARGV[1] 用户 id
-- ARGV[2] 标记 TTL（秒）

-- 1.业务守卫：不占坑 = 无可回滚（须在写 marker 之前，避免白白消耗令牌）
if redis.call('sismember', KEYS[3], ARGV[1]) == 0 then
    return 0
end

-- 2.幂等闸门：抢占本 token 的回滚权（SET NX EX 原子，EX 保证标记最终过期清理）
--   抢占失败 = 本 token 已回滚过（重复补偿 / 并发补偿），直接跳过返回 0
if not redis.call('set', KEYS[1], '1', 'EX', tonumber(ARGV[2]), 'NX') then
    return 0
end

-- 3.回滚库存：+1 抵消 seckill.lua 对应订单的 INCRBY -1
redis.call('incrby', KEYS[2], 1)

-- 4.回滚一人一单记录：移除用户，放行其重新下单
redis.call('srem', KEYS[3], ARGV[1])

-- 5.回滚成功返回 1
return 1
