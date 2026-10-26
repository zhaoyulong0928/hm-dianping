package com.hmdp;

import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.ShopBloomFilter;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import javax.annotation.Resource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillReconciliationTest extends SeckillIntegrationTestSupport {

    @Resource
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    @Test
    void shouldCompensateExpiredSuspectOnlyOnceAcrossRepeatedReconciliation() {
        Long voucherId = prepareVoucher(1);
        long userId = 7_300_000_003L;
        long orderId = 9_300_000_000_003L;
        prepareOccupied(voucherId, userId, orderId);
        String suspectKey = RedisConstants.SECKILL_RECON_SUSPECT_KEY + voucherId + ":" + userId + ":" + orderId;
        String rollbackToken = "recon:" + voucherId + ":" + userId;
        trackRedisKey(suspectKey);
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + rollbackToken);

        reconciliationTask.reconcile();
        assertThat(stringRedisTemplate.opsForValue().get(suspectKey)).isNotNull();
        assertThat(redisStock(voucherId)).isZero();
        assertThat(occupied(voucherId, userId)).isTrue();
        assertThat(orderCount(voucherId)).isZero();

        stringRedisTemplate.opsForValue().set(suspectKey,
                String.valueOf(System.currentTimeMillis() - 61_000L));
        reconciliationTask.reconcile();
        assertThat(redisStock(voucherId)).isEqualTo(1);
        assertThat(occupied(voucherId, userId)).isFalse();
        assertThat(stringRedisTemplate.hasKey(suspectKey)).isFalse();
        assertThat(orderCount(voucherId)).isZero();

        boolean repeatedRollback = voucherOrderService.rollbackSeckill(
                rollbackToken, voucherId, userId, String.valueOf(orderId));
        reconciliationTask.reconcile();
        assertThat(repeatedRollback).isFalse();
        assertThat(redisStock(voucherId)).isEqualTo(1);
        assertThat(occupied(voucherId, userId)).isFalse();
        assertThat(orderCount(voucherId)).isZero();
        System.out.printf("PHASE2_EVIDENCE case=reconciliation-idempotent runId=%s voucherId=%d userId=%d firstSuspect=true compensatedStock=1 repeatedRollback=%s finalStock=%d occupied=%s orders=%d%n",
                runId, voucherId, userId, repeatedRollback, redisStock(voucherId),
                occupied(voucherId, userId), orderCount(voucherId));
    }

    @Test
    void shouldNotOverwriteOrDeleteNewOrderSuspectWithOldOrderProcessing() {
        Long voucherId = prepareVoucher(1);
        long userId = 7_300_000_006L;
        long oldOrderId = 9_300_000_000_006L;
        long newOrderId = 9_300_000_000_007L;
        String oldSuspectKey = RedisConstants.SECKILL_RECON_SUSPECT_KEY
                + voucherId + ":" + userId + ":" + oldOrderId;
        String newSuspectKey = RedisConstants.SECKILL_RECON_SUSPECT_KEY
                + voucherId + ":" + userId + ":" + newOrderId;
        String oldOrderToken = "order:" + oldOrderId;
        String reconToken = "recon:" + voucherId + ":" + userId;
        trackRedisKey(oldSuspectKey);
        trackRedisKey(newSuspectKey);
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + oldOrderToken);
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + reconToken);

        prepareOccupied(voucherId, userId, oldOrderId);
        reconciliationTask.reconcile();
        assertThat(stringRedisTemplate.opsForValue().get(oldSuspectKey)).isNotNull();
        stringRedisTemplate.opsForValue().set(oldSuspectKey,
                String.valueOf(System.currentTimeMillis() - 61_000L));

        assertThat(voucherOrderService.rollbackSeckill(
                oldOrderToken, voucherId, userId, String.valueOf(oldOrderId))).isTrue();
        prepareOccupied(voucherId, userId, newOrderId);
        reconciliationTask.reconcile();
        assertThat(stringRedisTemplate.opsForValue().get(oldSuspectKey)).isNotNull();
        assertThat(stringRedisTemplate.opsForValue().get(newSuspectKey)).isNotNull();
        assertThat(occupied(voucherId, userId)).isTrue();
        assertThat(redisStock(voucherId)).isZero();

        boolean lateOldCompensation = voucherOrderService.rollbackSeckill(
                reconToken, voucherId, userId, String.valueOf(oldOrderId));
        assertThat(lateOldCompensation).isFalse();
        assertThat(stringRedisTemplate.opsForValue().get(newSuspectKey)).isNotNull();
        assertThat(occupied(voucherId, userId)).isTrue();
        assertThat(redisStock(voucherId)).isZero();

        stringRedisTemplate.opsForValue().set(newSuspectKey,
                String.valueOf(System.currentTimeMillis() - 61_000L));
        reconciliationTask.reconcile();
        assertThat(redisStock(voucherId)).isEqualTo(1);
        assertThat(occupied(voucherId, userId)).isFalse();
        assertThat(stringRedisTemplate.hasKey(newSuspectKey)).isFalse();
        assertThat(stringRedisTemplate.hasKey(oldSuspectKey)).isTrue();
        System.out.printf("PHASE2_EVIDENCE case=suspect-order-isolation runId=%s voucherId=%d userId=%d oldOrderId=%d newOrderId=%d lateOldCompensation=%s finalStock=%d oldSuspectKept=%s newSuspectDeleted=%s%n",
                runId, voucherId, userId, oldOrderId, newOrderId, lateOldCompensation,
                redisStock(voucherId), stringRedisTemplate.hasKey(oldSuspectKey),
                !stringRedisTemplate.hasKey(newSuspectKey));
    }
}
