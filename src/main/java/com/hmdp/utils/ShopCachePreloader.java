package com.hmdp.utils;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * 商铺缓存启动预热器。
 * <p>
 * 逻辑过期方案的缓存不会由读请求"首次回填"（缓存未命中时直接返回 null），
 * 因此必须在应用启动时把全量商铺以逻辑过期格式写入 Redis，
 * 否则未预热的商铺会被误判为"店铺不存在"。
 */
@Slf4j
@Component
public class ShopCachePreloader implements ApplicationRunner {

    /** 逻辑过期周期：与 queryById 中逻辑过期方案的 TTL 保持一致 */
    private static final long LOGICAL_EXPIRE_SECONDS = 20L;

    @Resource
    private IShopService shopService;

    @Resource
    private CacheClient cacheClient;

    @Override
    public void run(ApplicationArguments args) {
        List<Shop> shops = shopService.list();
        for (Shop shop : shops) {
            cacheClient.setWithLogicalExpire(
                    CACHE_SHOP_KEY + shop.getId(),
                    shop,
                    LOGICAL_EXPIRE_SECONDS,
                    TimeUnit.SECONDS);
        }
        log.info("商铺缓存预热完成，共 {} 个商铺", shops.size());
    }
}
