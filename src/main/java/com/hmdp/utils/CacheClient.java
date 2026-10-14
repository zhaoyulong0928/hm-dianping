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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_REBUILD_BUSY_MESSAGE;
import static com.hmdp.utils.RedisConstants.CACHE_REBUILD_MAX_RETRIES;
import static com.hmdp.utils.RedisConstants.CACHE_REBUILD_RETRY_MAX_MS;
import static com.hmdp.utils.RedisConstants.CACHE_REBUILD_RETRY_MIN_MS;
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
        // 2.缓存未命中（如被延迟双删清除）：查库自愈并回填，不能直接视为不存在
        if (StrUtil.isBlank(json)) {
            R r = dbFallback.apply(id);
            if (r == null) {
                // 数据库中也不存在，才返回未找到
                return null;
            }
            // 加锁防止删除瞬间多线程同时查库回填；抢锁失败说明已有线程在回填，直接返回即可
            RLock lock = redissonClient.getLock(LOCK_SHOP_KEY + id);
            if (lock.tryLock()) {
                try {
                    this.setWithLogicalExpire(key, r, time, unit);
                } finally {
                    lock.unlock();
                }
            }
            return r;
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
                if (newR == null) {
                    // 数据已删除/下架：删除缓存，避免写入 data=null 的脏缓存
                    // （Jackson non_null 会丢掉 data 字段，下次读取时反序列化抛 ClassCastException）
                    // 删掉 key 后，下次请求走 miss 自愈路径重新查库，返回"店铺不存在"，行为一致
                    stringRedisTemplate.delete(key);
                    return;
                }
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
        // 有界重试：抢锁失败则等待 30~50ms 后重查缓存，最多重试 CACHE_REBUILD_MAX_RETRIES 次，仍失败则降级
        for (int retry = 0; retry < CACHE_REBUILD_MAX_RETRIES; retry++) {
            boolean isLock = lock.tryLock();
            if (isLock) {
                try {
                    // 4.1.抢锁成功：先 double-check 缓存，等待期间别的线程可能已重建完成，避免重复查库
                    shopJson = stringRedisTemplate.opsForValue().get(key);
                    if (StrUtil.isNotBlank(shopJson)) {
                        return JSONUtil.toBean(shopJson, type);
                    }
                    if (shopJson != null) {
                        return null;
                    }
                    // 4.2.缓存仍不存在，查库重建缓存
                    R r = dbFallback.apply(id);
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
            // 4.3.抢锁失败：说明已有别的线程在重建缓存，等待 30~50ms 后重查缓存
            try {
                Thread.sleep(CACHE_REBUILD_RETRY_MIN_MS
                        + ThreadLocalRandom.current().nextLong(CACHE_REBUILD_RETRY_MAX_MS - CACHE_REBUILD_RETRY_MIN_MS + 1));
            } catch (InterruptedException e) {
                // 恢复中断标记并立即降级，避免线程收到停止信号后继续空转
                Thread.currentThread().interrupt();
                throw new CacheBusyException(CACHE_REBUILD_BUSY_MESSAGE);
            }
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, type);
            }
            if (shopJson != null) {
                return null;
            }
        }
        // 8.重试耗尽仍无法读到缓存：降级返回"系统繁忙"，不穿透到数据库
        throw new CacheBusyException(CACHE_REBUILD_BUSY_MESSAGE);
    }
}
