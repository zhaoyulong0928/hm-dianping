package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    /** 商户布隆过滤器 key：拦截"从未存在过"的店铺 id，防缓存穿透（Redisson RBloomFilter 位图） */
    public static final String BLOOM_SHOP_KEY = "bloom:shop";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    /** 缓存重建互斥锁的最大重试次数：每次重试等待 30~50ms 后重查缓存，超过次数则降级 */
    public static final int CACHE_REBUILD_MAX_RETRIES = 3;
    /** 缓存重建抢锁失败后单次重试的等待下限（毫秒） */
    public static final long CACHE_REBUILD_RETRY_MIN_MS = 30L;
    /** 缓存重建抢锁失败后单次重试的等待上限（毫秒） */
    public static final long CACHE_REBUILD_RETRY_MAX_MS = 50L;
    /** 缓存重建抢锁超时后的降级提示文案 */
    public static final String CACHE_REBUILD_BUSY_MESSAGE = "商户信息加载中，请稍后重试";

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String SECKILL_BEGIN_KEY = "seckill:begin:";
    public static final String SECKILL_END_KEY = "seckill:end:";
    /** 秒杀回滚标记 key 前缀：seckill:rollback:{rollbackToken}，同一令牌的补偿只生效一次 */
    public static final String SECKILL_ROLLBACK_MARKER_KEY = "seckill:rollback:";
    /** 秒杀对账嫌疑 key 前缀：seckill:recon:suspect:{vid}:{uid}:{orderId}，value 为首见时间戳 */
    public static final String SECKILL_RECON_SUSPECT_KEY = "seckill:recon:suspect:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
