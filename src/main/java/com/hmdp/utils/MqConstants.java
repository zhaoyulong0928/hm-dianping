package com.hmdp.utils;

public class MqConstants {

    // ============ 秒杀订单消息 ============

    public static final String SECKILL_ORDER_EXCHANGE = "seckill.order.exchange";
    public static final String SECKILL_ORDER_QUEUE = "seckill.order.queue";
    public static final String SECKILL_ORDER_ROUTING_KEY = "seckill.order";

    public static final String SECKILL_DLX_EXCHANGE = "seckill.dlx.exchange";
    public static final String SECKILL_DLX_QUEUE = "seckill.dlx.queue";
    public static final String SECKILL_DLX_ROUTING_KEY = "seckill.dlx";

    // ============ 缓存删除消息（延迟双删升级为 MQ 可靠删除） ============

    /** 缓存删除交换机 */
    public static final String CACHE_DELETE_EXCHANGE = "cache.delete.exchange";
    /** 缓存删除交换机路由键（生产者发送时使用） */
    public static final String CACHE_DELETE_DELAY_ROUTING_KEY = "cache.delete.delay";
    /** 延迟队列：消息在此停留 CACHE_DELETE_DELAY_MILLIS 毫秒后过期，经死信转入删除队列 */
    public static final String CACHE_DELETE_DELAY_QUEUE = "cache.delete.delay.queue";
    /** 删除队列路由键：延迟队列过期消息凭此路由进入删除队列 */
    public static final String CACHE_DELETE_ROUTING_KEY = "cache.delete";
    /** 实际删除队列：消费者从队列消费并执行 Redis 删除 */
    public static final String CACHE_DELETE_QUEUE = "cache.delete.queue";
    /** 延迟双删间隔：需大于"读线程查库 + 回写缓存"耗时（与旧本地线程池版本保持一致） */
    public static final long CACHE_DELETE_DELAY_MILLIS = 500L;

    /** 删除队列的死信交换机：删除失败重试耗尽后进入，供告警 / 人工补偿 */
    public static final String CACHE_DLX_EXCHANGE = "cache.dlx.exchange";
    public static final String CACHE_DLX_QUEUE = "cache.dlx.queue";
    public static final String CACHE_DLX_ROUTING_KEY = "cache.dlx";
}