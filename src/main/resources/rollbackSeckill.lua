-- 幂等回滚秒杀预扣（库存 + 一人一单占坑）
-- 归属校验 + 幂等闸门：仅当前占坑仍属于 expectedOrderId 时，才允许回滚。
-- 防止旧请求补偿迟到后，误撤销同用户同券新请求的占坑。
--
-- KEYS[1] 回滚标记 key（seckill:rollback:{rollbackToken}）
-- KEYS[2] 库存 key（seckill:stock:{voucherId}）
-- KEYS[3] 一人一单占坑 Hash（seckill:order:{voucherId}，field=userId，value=orderId）
-- ARGV[1] 用户 id（字符串）
-- ARGV[2] 期望回滚的订单 id（字符串，禁止转成 Lua 数值）
-- ARGV[3] 标记 TTL（秒）

-- 1.订单归属守卫：占坑不存在或已属于其他订单时，无可回滚之物
local currentOrderId = redis.call('hget', KEYS[3], ARGV[1])
if not currentOrderId or currentOrderId ~= ARGV[2] then
    return 0
end

-- 2.幂等闸门：同一 rollbackToken 只能回滚一次
if not redis.call('set', KEYS[1], '1', 'EX', tonumber(ARGV[3]), 'NX') then
    return 0
end

-- 3.回滚库存并移除当前订单占坑
redis.call('incrby', KEYS[2], 1)
redis.call('hdel', KEYS[3], ARGV[1])
return 1
