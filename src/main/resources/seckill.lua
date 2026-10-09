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

-- 3.校验秒杀时间（用 Redis 服务端时间，避免各应用服务器时钟不一致）
-- 3.1.秒杀尚未开始，返回3
local now = tonumber(redis.call('time')[1])
local beginTime = tonumber(redis.call('get', beginKey))
if(beginTime and now < beginTime) then
    return 3
end
-- 3.2.秒杀已经结束，返回4
local endTime = tonumber(redis.call('get', endKey))
if(endTime and now > endTime) then
    return 4
end

-- 4.脚本业务
-- 4.1.判断库存是否充足 get stockKey（key 不存在时视为 0，避免 tonumber(nil) 报错）
if(tonumber(redis.call('get', stockKey) or 0) <= 0) then
    -- 4.2.库存不足，返回1
    return 1
end
-- 4.3.判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 4.4.存在，说明是重复下单，返回2
    return 2
end
-- 4.5.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 4.6.下单（保存用户）sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 4.7.发送消息到队列中， XADD stream.orders * k1 v1 k2 v2 ...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0