package com.hmdp.utils;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.BLOOM_SHOP_KEY;

/**
 * 商户布隆过滤器：缓存穿透的主动防线。
 * <p>
 * 在查缓存、查库之前拦截"系统里从未存在过"的店铺 id，这类请求既不查缓存、不查数据库，
 * 也不会写入空值 key（避免恶意 id 轰炸 Redis 内存）。
 * <p>
 * 已知限制（均由读路径兜底，可接受）：
 * 1. 布隆存在约 1% 误判："可能存在"但实际不存在的请求会走到查库，返回不存在；
 * 2. 布隆不支持删除：已删除店铺仍会"可能存在"，同样由查库返回不存在兜住；
 * 3. 运行期若 Redis 被清空（如 FLUSHDB），contains 会全部返回 false，
 *    表现为所有店铺查询被拒（过度拦截而非缓存击穿，数据库反而更安全），重启应用即可自愈。
 */
@Slf4j
@Component
public class ShopBloomFilter implements ApplicationRunner {

    /** 预期插入元素数：按业务量级预留增长空间 */
    private static final long EXPECTED_INSERTIONS = 100_000L;

    /** 误判率 1%：位图约 115KB、约 7 个哈希函数（bits = -n·ln(p)/(ln2)^2） */
    private static final double FALSE_PROBABILITY = 0.01;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private ShopMapper shopMapper;

    /** 由启动线程初始化，请求线程只读；volatile 保证初始化结果对请求线程立即可见 */
    private volatile RBloomFilter<Long> bloomFilter;

    /**
     * 启动时初始化并全量加载店铺 id。
     * tryInit 仅在布隆 key 不存在时写入参数；add 是幂等操作（重复置位同一批 bit），
     * 因此每次启动都全量补充是安全的，同时可自愈"布隆 key 被清 / 数据库重导"的场景。
     */
    @Override
    public void run(ApplicationArguments args) {
        // 初始化布隆过滤器
        RBloomFilter<Long> filter = redissonClient.getBloomFilter(BLOOM_SHOP_KEY);
        // 初始化布隆过滤器，仅在 key 不存在时写入参数
        filter.tryInit(EXPECTED_INSERTIONS, FALSE_PROBABILITY);
        // 全量加载店铺 id 到布隆过滤器
        List<Shop> shops = shopMapper.selectList(null);
        // 重复置位同一批 bit，对后续查询无影响
        shops.forEach(shop -> filter.add(shop.getId()));
        // 初始化完成，将布隆过滤器赋值给 volatile 变量
        this.bloomFilter = filter;
        log.info("布隆过滤器[{}]初始化完成，已加载 {} 个店铺 id", BLOOM_SHOP_KEY, shops.size());
    }

    /**
     * id 是否"可能存在"：返回 false 表示一定不存在，可直接拒绝，不查缓存不查库。
     */
    public boolean contains(Long id) {
        RBloomFilter<Long> filter = this.bloomFilter;
        if (filter == null) {
            // 应用刚启动、初始化尚未完成的极短窗口：放行，退回读路径自身防线
            return true;
        }
        return filter.contains(id);
    }

    /**
     * 新增店铺后同步登记，避免新店铺被误拦（当前项目暂无新增店铺接口，预留维护入口）。
     */
    public void add(Long id) {
        bloomFilter.add(id);
    }
}
