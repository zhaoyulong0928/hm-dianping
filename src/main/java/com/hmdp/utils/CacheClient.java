package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    /** Redisson 客户端：缓存重建时用分布式锁保证"同一时刻只有一个线程重建"，避免缓存击穿打爆 DB */
    private final RedissonClient redissonClient;

    /** 缓存异步重建线程池：逻辑过期方案的缓存重建在独立线程中完成，避免阻塞请求线程 */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate, RedissonClient redissonClient) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    // 设置逻辑过期时间
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    // 解决缓存穿透问题，使用缓存空值
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的是否是空值
        if (json != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }
        // 6.存在，写入redis
        this.set(key, r, time, unit);
        return r;
    }
    // 解决缓存击穿问题，使用逻辑过期时间
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在（逻辑过期方案依赖预热，未命中直接视为不存在）
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 3.命中，先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 4.1.未过期，直接返回缓存数据（保证可用性）
            return r;
        }
        // 4.2.已过期，需要缓存重建
        // 注意：Redisson 的 RLock 是线程绑定的，谁 tryLock 谁才能 unlock，不能跨线程释放。
        // 因此"拿锁 + 查库重建 + 释放锁"三者必须放在同一个异步线程里串行执行。
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            RLock lock = redissonClient.getLock(LOCK_SHOP_KEY + id);
            boolean isLock = lock.tryLock();
            if (!isLock) {
                // 已有线程在重建，本线程直接跳过，避免重复查库
                return;
            }
            try {
                // 查询数据库并重建缓存
                R newR = dbFallback.apply(id);
                this.setWithLogicalExpire(key, newR, time, unit);
            } finally {
                lock.unlock();
            }
        });
        // 5.缓存重建期间，先把过期的旧数据返回给用户
        return r;
    }
    // 解决缓存击穿问题，使用互斥锁
    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断命中的是否是空值
        if (shopJson != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.缓存未命中，用分布式锁保证同一时刻只有一个线程去查库重建
        RLock lock = redissonClient.getLock(LOCK_SHOP_KEY + id);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            // 4.1.抢锁失败：说明已有别的线程在重建缓存，本线程休眠后递归重试
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
        }
        // 4.2.抢锁成功，查库重建缓存
        R r;
        try {
            r = dbFallback.apply(id);
            // 5.数据库不存在
            if (r == null) {
                // 将空值写入redis，避免缓存穿透
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6.存在，写入redis
            this.set(key, r, time, unit);
            return r;
        } finally {
            // 7.释放锁（Redisson 会校验持有线程，不会误删他人持有的锁）
            lock.unlock();
        }
    }
}
