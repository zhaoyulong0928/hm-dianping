package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.CacheDeleteMsg;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.ShopBloomFilter;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private ShopBloomFilter shopBloomFilter;

    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * 本地兜底延迟删除线程池：与 MQ 路径组成"双保险"，仅在进程存活时兜住"消息到达 broker 之前失败"的盲区
     * （同步发送异常 / broker nack / 路由失败）。删除 key 幂等，与 MQ 路径重复执行无副作用。
     * 核心线程空闲 60s 后回收、允许核心线程超时，避免长期占用；应用关闭时优雅停止。
     */
    private static final ScheduledThreadPoolExecutor CACHE_DELETE_FALLBACK_EXECUTOR =
            createCacheDeleteFallbackExecutor();

    /** 创建本地兜底延迟删除线程池：核心线程空闲 60s 后被回收，避免长期占用 */
    private static ScheduledThreadPoolExecutor createCacheDeleteFallbackExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        // 核心线程空闲存活时间
        executor.setKeepAliveTime(60L, TimeUnit.SECONDS);
        // 允许核心线程超时回收（默认 false，核心线程永不回收）
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    @Override
    public Result queryById(Long id) {
        // 缓存穿透防线一：参数基础校验（主动防御），拦截明显非法的 id
        if (id == null || id <= 0) {
            return Result.fail("店铺不存在！");
        }
        // 缓存穿透防线二：布隆过滤器（ShopBloomFilter），拦截"从未存在过"的 id，不查缓存、不查库
        if (!shopBloomFilter.contains(id)) {
            return Result.fail("店铺不存在！");
        }
        // 缓存穿透防线三 + 缓存击穿处理：逻辑过期方案（ShopCachePreloader 启动预热，miss 查库自愈回填）
        // 备选读方案（穿透-空值缓存 / 击穿-互斥锁）：
        // Shop shop = cacheClient
        //         .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        Shop shop = cacheClient
                .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        // 7.返回
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.只更新数据库，此时事务尚未提交
        updateById(shop);
        // 2.把删缓存注册到"事务提交之后"执行，保证先提交、再删缓存
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                String key = CACHE_SHOP_KEY + id;
                // 第一次删除：清掉缓存中的旧值（快速路径，大部分脏缓存即刻被清掉）
                stringRedisTemplate.delete(key);
                // 第二次删除（延迟约 500ms）：兜住"提交前读到旧值、删除后才回写"的脏数据。
                // 采用"双保险"两条路径（两者都无条件执行，并非主备/条件触发关系），互为盲区补位：
                // 两条路径不会冲突——删除 key 幂等（key 不存在时 DEL 返回 0、不报错），且 Redis 命令串行执行，
                // 因此谁先谁后、甚至同时到达，最终结果都是"key 被删掉"，最坏只是多一次无副作用的 DEL。

                // 路径一：本地延迟删除。覆盖"消息到达 broker 之前失败"的盲区
                // （同步发送异常 / broker nack / 路由失败）——此刻进程仍存活，本地任务必定执行。
                CACHE_DELETE_FALLBACK_EXECUTOR.schedule(
                        () -> stringRedisTemplate.delete(key),
                        MqConstants.CACHE_DELETE_DELAY_MILLIS, TimeUnit.MILLISECONDS);

                // 路径二：RabbitMQ 延迟队列（TTL + 死信）。覆盖"本地任务执行前进程宕机"的盲区
                // ——消息已持久化在 broker，重启后仍会消费执行；消费失败重试耗尽进入 cache.dlx.queue。
                // 实际触发时刻：本地为精确的 T+500ms，MQ 因 broker 投递与消费耗时略晚，两者并非同一瞬间执行。
                try {
                    rabbitTemplate.convertAndSend(
                            MqConstants.CACHE_DELETE_EXCHANGE, //交换机名
                            MqConstants.CACHE_DELETE_DELAY_ROUTING_KEY, //路由键
                            new CacheDeleteMsg(key), //消息
                            new CorrelationData(key)); //消息标识，供 confirm 回调定位失败消息
                } catch (Exception e) {
                    // 路径一已无条件执行过，此处发送失败不影响最终删除结果，仅记日志、不阻断主流程
                    log.error("缓存延迟删除消息发送失败，路径一本地兜底已覆盖: key={}", key, e);
                }
                // 唯一的失效窗口（无法用当前方案消除）：本地任务执行前进程崩溃 且 MQ 发送失败（两者皆失）。
            }
        });
        return Result.ok();
    }

    /** 应用关闭时优雅停止本地兜底删除线程池：等待未执行的延迟删除任务执行完 */
    @PreDestroy
    public void shutdownCacheDeleteFallbackExecutor() {
        CACHE_DELETE_FALLBACK_EXECUTOR.shutdown();
        try {
            if (!CACHE_DELETE_FALLBACK_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                CACHE_DELETE_FALLBACK_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            CACHE_DELETE_FALLBACK_EXECUTOR.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
