package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.task.SeckillActivityRedisInitializer;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.ShopBloomFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * 验证 afterCommit 的异常边界：数据库已提交时，初始化异常不得变成接口错误。
 * 需要 MySQL / Redis（seckill-test profile 的环境变量）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillActivityInitFailureTest {

    @Autowired
    private IVoucherService voucherService;
    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private IVoucherOrderService voucherOrderService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @MockBean
    private SeckillActivityRedisInitializer seckillActivityRedisInitializer;
    @MockBean
    private RabbitTemplate rabbitTemplate;
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    private final List<Long> createdVoucherIds = new ArrayList<>();

    @Test
    void shouldKeepCommittedDatabaseAndNotThrowWhenInitializerFails() {
        when(seckillActivityRedisInitializer.publishNewActivity(any(SeckillVoucher.class)))
                .thenThrow(new RuntimeException("redis down"));

        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle("init-failure-" + UUID.randomUUID());
        voucher.setSubTitle("test");
        voucher.setRules("test");
        voucher.setPayValue(1L);
        voucher.setActualValue(1L);
        voucher.setType(1);
        voucher.setStock(5);
        voucher.setBeginTime(LocalDateTime.now().minusHours(1));
        voucher.setEndTime(LocalDateTime.now().plusHours(1));

        // 不应抛异常：afterCommit 内部已兜底
        voucherService.addSeckillVoucher(voucher);
        createdVoucherIds.add(voucher.getId());

        assertThat(voucherService.getById(voucher.getId())).isNotNull();
        assertThat(seckillVoucherService.getById(voucher.getId())).isNotNull();
        assertThat(stringRedisTemplate.hasKey(
                RedisConstants.SECKILL_STOCK_KEY + voucher.getId())).isFalse();
    }

    @AfterEach
    void cleanup() {
        for (Long vid : createdVoucherIds) {
            voucherOrderService.remove(new QueryWrapper<VoucherOrder>().eq("voucher_id", vid));
            seckillVoucherService.removeById(vid);
            voucherService.removeById(vid);
            stringRedisTemplate.delete(java.util.Arrays.asList(
                    RedisConstants.SECKILL_STOCK_KEY + vid,
                    RedisConstants.SECKILL_BEGIN_KEY + vid,
                    RedisConstants.SECKILL_END_KEY + vid,
                    "seckill:order:" + vid));
        }
        createdVoucherIds.clear();
    }
}
