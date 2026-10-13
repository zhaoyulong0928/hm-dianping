package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
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
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /** 延迟删除专用线程池：执行"第二次删除"，避免阻塞业务线程 */
    private static final ScheduledThreadPoolExecutor DELAY_DELETE_EXECUTOR =
            createDelayDeleteExecutor();

    /** 延迟双删的间隔：需大于"读线程查库 + 回写缓存"的耗时 */
    private static final long DELAY_DELETE_MILLIS = 500L;

    /** 创建延迟删除线程池：核心线程空闲 60s 后被回收，避免长期占用 */
    private static ScheduledThreadPoolExecutor createDelayDeleteExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
        // 核心线程空闲存活时间
        executor.setKeepAliveTime(60L, TimeUnit.SECONDS);
        // 允许核心线程超时回收（默认 false，核心线程永不回收）
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透， （被动）缓存空对象，布隆过滤。（主动）增强id的复杂度，避免被猜测id规律，做好数据的基础格式校验，加强用户权限校验，做好热点参数限流
        // Shop shop = cacheClient
        //         .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿（依赖 ShopCachePreloader 启动预热，未预热的 key 会返回 null）
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
                // 第一次删除：清掉缓存中的旧值
                stringRedisTemplate.delete(key);
                // 第二次删除（延迟）：兜住"提交前读到旧值、删除后才回写"的脏数据
                DELAY_DELETE_EXECUTOR.schedule(
                        () -> stringRedisTemplate.delete(key),
                        DELAY_DELETE_MILLIS, TimeUnit.MILLISECONDS);
            }
        });
        return Result.ok();
    }

    /** 应用关闭时优雅停止线程池：等待未执行的延迟删除任务执行完 */
    @PreDestroy
    public void shutdownDelayDeleteExecutor() {
        DELAY_DELETE_EXECUTOR.shutdown();
        try {
            if (!DELAY_DELETE_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                DELAY_DELETE_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            DELAY_DELETE_EXECUTOR.shutdownNow();
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
