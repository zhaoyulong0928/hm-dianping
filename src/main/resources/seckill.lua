-- 注意：本脚本同时操作 voucherId / userId 派生的多种 key，未使用 hash tag，仅适用单机 Redis；
-- 若迁移 Redis Cluster，需为 key 增加 {voucherId} 形式的 hash tag 并重新评估脚本拆分
-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId
-- 2.3.秒杀开始时间key
local beginKey = 'seckill:begin:' .. voucherId
-- 2.4.秒杀结束时间key
local endKey = 'seckill:end:' .. voucherId
-- 2.5.用户级限流key
local freqKey = 'seckill:freq:' .. userId

-- 3.用户级限流：每用户每秒最多放行 1 次（固定窗口 INCR + EXPIRE），超限返回 5
-- 放在所有校验之前：同一用户并行/高频的脚本请求（如同一瞬间并行抢多张券），
-- 在时间/库存/一人一单判断之前直接被拒，不产生任何业务写操作
local freqCnt = tonumber(redis.call('incr', freqKey))
if freqCnt == 1 then
    redis.call('expire', freqKey, 1)
end
if freqCnt > 1 then
    return 5
end

-- 4.校验秒杀时间（用 Redis 服务端时间，避免各应用服务器时钟不一致）
-- 4.1.秒杀尚未开始，返回3
local now = tonumber(redis.call('time')[1])
local beginTime = tonumber(redis.call('get', beginKey))
if(beginTime and now < beginTime) then
    return 3
end
-- 4.2.秒杀已经结束，返回4
local endTime = tonumber(redis.call('get', endKey))
if(endTime and now > endTime) then
    return 4
end

-- 5.脚本业务
-- 5.1.判断库存是否充足 get stockKey（key 不存在时视为 0，避免 tonumber(nil) 报错）
if(tonumber(redis.call('get', stockKey) or 0) <= 0) then
    -- 5.2.库存不足，返回1
    return 1
end
-- 5.3.判断用户是否下单：Hash field=userId，value=字符串 orderId
if(redis.call('hexists', orderKey, userId) == 1) then
    -- 5.4.存在，说明是重复下单，返回2
    return 2
end
-- 5.5.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 5.6.保存占坑及订单归属；orderId 保持字符串，不转数值
redis.call('hset', orderKey, userId, orderId)
-- 5.7.秒杀成功返回0（下单消息改由 Java 侧投递到 RabbitMQ）
return 0
