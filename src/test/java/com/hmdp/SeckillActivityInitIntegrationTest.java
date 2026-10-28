package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.task.SeckillActivityRedisInitializer;
import com.hmdp.task.SeckillActivityRedisInitializer.PublishResult;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.ShopBloomFilter;
import com.hmdp.utils.UserHolder;
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
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * 需要 MySQL / Redis / RabbitMQ（seckill-test profile 的环境变量）。
 * 事务提交类用例不依赖 MQ，RabbitTemplate 已 mock。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillActivityInitIntegrationTest {

    @Autowired
    private IVoucherService voucherService;
    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private IVoucherOrderService voucherOrderService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private SeckillActivityRedisInitializer initializer;

    @MockBean
    private RabbitTemplate rabbitTemplate;
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    private final List<Long> createdVoucherIds = new ArrayList<>();
    private final Set<Long> usedUserIds = new HashSet<>();

    @Test
    @Transactional
    void shouldNotPublishBeforeOuterTransactionCommits() {
        Long vid = createVoucher("pre-commit", 5);

        assertThat(stringRedisTemplate.hasKey(RedisConstants.SECKILL_STOCK_KEY + vid)).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisConstants.SECKILL_BEGIN_KEY + vid)).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisConstants.SECKILL_END_KEY + vid)).isFalse();
    }

    @Test
    void shouldPublishAfterRealCommit() {
        LocalDateTime begin = LocalDateTime.now().minusHours(1);
        LocalDateTime end = LocalDateTime.now().plusHours(1);
        Long vid = createVoucher("real-commit", 5, begin, end);

        assertThat(stringRedisTemplate.opsForValue()
                .get(RedisConstants.SECKILL_STOCK_KEY + vid)).isEqualTo("5");
        assertThat(stringRedisTemplate.opsForValue()
                .get(RedisConstants.SECKILL_BEGIN_KEY + vid))
                .isEqualTo(String.valueOf(begin.atZone(ZoneId.systemDefault()).toEpochSecond()));
        assertThat(stringRedisTemplate.opsForValue()
                .get(RedisConstants.SECKILL_END_KEY + vid))
                .isEqualTo(String.valueOf(end.atZone(ZoneId.systemDefault()).toEpochSecond()));
    }

    @Test
    void shouldNotPublishWhenDatabaseRollsBack() {
        long vouchersBefore = voucherService.count();
        long stockKeysBefore = countKeys(RedisConstants.SECKILL_STOCK_KEY + "*");

        Voucher voucher = buildVoucher("rollback-" + UUID.randomUUID(), 5,
                LocalDateTime.now().minusHours(1), LocalDateTime.now().plusHours(1));
        voucher.setTitle(null); // 违反 tb_voucher.title NOT NULL，触发事务回滚

        assertThatThrownBy(() -> voucherService.addSeckillVoucher(voucher))
                .isInstanceOf(RuntimeException.class);

        assertThat(voucherService.count()).isEqualTo(vouchersBefore);
        assertThat(countKeys(RedisConstants.SECKILL_STOCK_KEY + "*")).isEqualTo(stockKeysBefore);
    }

    @Test
    void shouldNotOverwriteExistingStockOnRepeatedPublish() {
        Long vid = createVoucher("idempotent", 5);
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + vid;
        stringRedisTemplate.opsForValue().set(stockKey, "1");

        SeckillVoucher seckillVoucher = seckillVoucherService.getById(vid);
        PublishResult result = initializer.publishNewActivity(seckillVoucher);

        assertThat(result).isEqualTo(PublishResult.ALREADY_EXISTS);
        assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("1");
    }

    @Test
    void shouldRejectWhenRequiredKeyMissing() {
        Long vid = createVoucher("missing-key", 5);
        long userId = 7_400_000_001L;
        usedUserIds.add(userId);

        String stockKey = RedisConstants.SECKILL_STOCK_KEY + vid;
        String beginKey = RedisConstants.SECKILL_BEGIN_KEY + vid;
        String endKey = RedisConstants.SECKILL_END_KEY + vid;

        Map<String, String> originals = new HashMap<>();
        originals.put(stockKey, stringRedisTemplate.opsForValue().get(stockKey));
        originals.put(beginKey, stringRedisTemplate.opsForValue().get(beginKey));
        originals.put(endKey, stringRedisTemplate.opsForValue().get(endKey));

        for (String key : originals.keySet()) {
            stringRedisTemplate.delete(key);
            Result result = seckill(vid, userId);
            assertThat(result.getSuccess()).isFalse();
            assertThat(result.getErrorMsg()).isEqualTo("活动未就绪，请稍后再试");
            stringRedisTemplate.opsForValue().set(key, originals.get(key));
        }

        assertThat(stringRedisTemplate.opsForHash()
                .hasKey("seckill:order:" + vid, String.valueOf(userId))).isFalse();
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void shouldDistinguishZeroStockFromMissingKey() {
        Long vid = createVoucher("sold-out", 5);
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + vid;

        stringRedisTemplate.opsForValue().set(stockKey, "0");
        long userA = 7_400_000_002L;
        long userB = 7_400_000_003L;
        usedUserIds.add(userA);
        usedUserIds.add(userB);

        Result soldOut = seckill(vid, userA);
        assertThat(soldOut.getSuccess()).isFalse();
        assertThat(soldOut.getErrorMsg()).isEqualTo("库存不足");

        stringRedisTemplate.delete(stockKey);
        Result notReady = seckill(vid, userB);
        assertThat(notReady.getSuccess()).isFalse();
        assertThat(notReady.getErrorMsg()).isEqualTo("活动未就绪，请稍后再试");
    }

    @Test
    void shouldNotRestoreMissingStockInStaticRepair() {
        Long vid = createVoucher("no-stock-recall", 5);
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + vid;
        stringRedisTemplate.delete(stockKey);

        PublishResult result = initializer.repairStaticState(seckillVoucherService.getById(vid));

        assertThat(result).isEqualTo(PublishResult.SKIPPED_UNSAFE);
        assertThat(stringRedisTemplate.hasKey(stockKey)).isFalse();
    }

    @Test
    void shouldRepairTimeKeysOnly() {
        LocalDateTime begin = LocalDateTime.now().minusHours(1);
        LocalDateTime end = LocalDateTime.now().plusHours(1);
        Long vid = createVoucher("time-repair", 5, begin, end);

        String beginKey = RedisConstants.SECKILL_BEGIN_KEY + vid;
        String endKey = RedisConstants.SECKILL_END_KEY + vid;
        stringRedisTemplate.delete(beginKey);
        stringRedisTemplate.delete(endKey);

        PublishResult result = initializer.repairStaticState(seckillVoucherService.getById(vid));

        assertThat(result).isEqualTo(PublishResult.PUBLISHED);
        assertThat(stringRedisTemplate.opsForValue().get(beginKey))
                .isEqualTo(String.valueOf(begin.atZone(ZoneId.systemDefault()).toEpochSecond()));
        assertThat(stringRedisTemplate.opsForValue().get(endKey))
                .isEqualTo(String.valueOf(end.atZone(ZoneId.systemDefault()).toEpochSecond()));
    }

    @Test
    void shouldRejectInvalidParamsBeforeAnyWrite() {
        long vouchersBefore = voucherService.count();

        assertInvalid(v -> v.setBeginTime(null));
        assertInvalid(v -> v.setEndTime(null));
        assertInvalid(v -> v.setBeginTime(LocalDateTime.now().plusHours(2)));
        assertInvalid(v -> v.setStock(null));
        assertInvalid(v -> v.setStock(-1));

        assertThat(voucherService.count()).isEqualTo(vouchersBefore);
    }

    private void assertInvalid(java.util.function.Consumer<Voucher> mutation) {
        Voucher voucher = buildVoucher("invalid-" + UUID.randomUUID(), 5,
                LocalDateTime.now().minusHours(1), LocalDateTime.now().plusHours(1));
        mutation.accept(voucher);
        assertThatThrownBy(() -> voucherService.addSeckillVoucher(voucher))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Result seckill(Long voucherId, long userId) {
        UserDTO user = new UserDTO();
        user.setId(userId);
        UserHolder.saveUser(user);
        try {
            return voucherOrderService.seckillVoucher(voucherId);
        } finally {
            UserHolder.removeUser();
        }
    }

    private Long createVoucher(String tag, int stock) {
        return createVoucher(tag, stock,
                LocalDateTime.now().minusHours(1), LocalDateTime.now().plusHours(1));
    }

    private Long createVoucher(String tag, int stock, LocalDateTime begin, LocalDateTime end) {
        Voucher voucher = buildVoucher("init-test-" + tag + "-" + UUID.randomUUID(), stock, begin, end);
        voucherService.addSeckillVoucher(voucher);
        createdVoucherIds.add(voucher.getId());
        return voucher.getId();
    }

    private Voucher buildVoucher(String title, int stock, LocalDateTime begin, LocalDateTime end) {
        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle(title);
        voucher.setSubTitle("test");
        voucher.setRules("test");
        voucher.setPayValue(1L);
        voucher.setActualValue(1L);
        voucher.setType(1);
        voucher.setStock(stock);
        voucher.setBeginTime(begin);
        voucher.setEndTime(end);
        return voucher;
    }

    private long countKeys(String pattern) {
        Set<String> keys = stringRedisTemplate.keys(pattern);
        return keys == null ? 0L : keys.size();
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
        for (Long userId : usedUserIds) {
            stringRedisTemplate.delete("seckill:freq:" + userId);
        }
        createdVoucherIds.clear();
        usedUserIds.clear();
    }
}
