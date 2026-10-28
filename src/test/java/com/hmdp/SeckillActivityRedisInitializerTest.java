package com.hmdp;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.task.SeckillActivityRedisInitializer;
import com.hmdp.task.SeckillActivityRedisInitializer.PublishResult;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 纯单元测试：不依赖 MySQL / Redis / RabbitMQ。
 */
class SeckillActivityRedisInitializerTest {

    private static final Long VOUCHER_ID = 9_100_000_001L;
    private static final String BEGIN_KEY = RedisConstants.SECKILL_BEGIN_KEY + VOUCHER_ID;
    private static final String END_KEY = RedisConstants.SECKILL_END_KEY + VOUCHER_ID;
    private static final String STOCK_KEY = RedisConstants.SECKILL_STOCK_KEY + VOUCHER_ID;

    private StringRedisTemplate template;
    private ValueOperations<String, String> ops;
    private SeckillActivityRedisInitializer initializer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        template = mock(StringRedisTemplate.class);
        ops = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(ops);
        initializer = new SeckillActivityRedisInitializer();
        ReflectionTestUtils.setField(initializer, "stringRedisTemplate", template);
    }

    @Test
    void publishWritesTimeKeysBeforeStock() {
        when(ops.setIfAbsent(anyString(), anyString())).thenReturn(true);

        PublishResult result = initializer.publishNewActivity(voucher(10));

        assertThat(result).isEqualTo(PublishResult.PUBLISHED);
        InOrder order = inOrder(ops);
        order.verify(ops).setIfAbsent(startsWith(RedisConstants.SECKILL_BEGIN_KEY), anyString());
        order.verify(ops).setIfAbsent(startsWith(RedisConstants.SECKILL_END_KEY), anyString());
        order.verify(ops).setIfAbsent(startsWith(RedisConstants.SECKILL_STOCK_KEY), anyString());
    }

    @Test
    void publishDoesNotWriteStockWhenTimeStageThrows() {
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_BEGIN_KEY), anyString())).thenReturn(true);
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_END_KEY), anyString()))
                .thenThrow(new RuntimeException("redis down"));

        PublishResult result = initializer.publishNewActivity(voucher(10));

        assertThat(result).isEqualTo(PublishResult.FAILED);
        verify(ops, never()).setIfAbsent(startsWith(RedisConstants.SECKILL_STOCK_KEY), anyString());
    }

    @Test
    void publishReturnsFailedWhenStockWriteThrows() {
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_BEGIN_KEY), anyString())).thenReturn(true);
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_END_KEY), anyString())).thenReturn(true);
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_STOCK_KEY), anyString()))
                .thenThrow(new RuntimeException("redis down"));

        PublishResult result = initializer.publishNewActivity(voucher(10));

        assertThat(result).isEqualTo(PublishResult.FAILED);
    }

    @Test
    void publishReturnsAlreadyExistsWhenKeysExist() {
        when(ops.setIfAbsent(anyString(), anyString())).thenReturn(false);

        PublishResult result = initializer.publishNewActivity(voucher(10));

        assertThat(result).isEqualTo(PublishResult.ALREADY_EXISTS);
        verify(ops, never()).set(anyString(), anyString());
    }

    @Test
    void publishRejectsNullAndInvalidDataWithoutTouchingRedis() {
        assertThat(initializer.publishNewActivity(null)).isEqualTo(PublishResult.FAILED);

        SeckillVoucher noBegin = voucher(10).setBeginTime(null);
        assertThat(initializer.publishNewActivity(noBegin)).isEqualTo(PublishResult.FAILED);

        SeckillVoucher reversed = voucher(10)
                .setBeginTime(LocalDateTime.now().plusHours(2))
                .setEndTime(LocalDateTime.now().plusHours(1));
        assertThat(initializer.publishNewActivity(reversed)).isEqualTo(PublishResult.FAILED);

        SeckillVoucher negativeStock = voucher(-1);
        assertThat(initializer.publishNewActivity(negativeStock)).isEqualTo(PublishResult.FAILED);

        verifyNoInteractions(ops);
    }

    @Test
    void repairOnlyWritesTimeKeysAndNeverStock() {
        when(ops.setIfAbsent(anyString(), anyString())).thenReturn(true);
        when(template.hasKey(STOCK_KEY)).thenReturn(false);

        PublishResult result = initializer.repairStaticState(voucher(10));

        assertThat(result).isEqualTo(PublishResult.SKIPPED_UNSAFE);
        verify(ops).setIfAbsent(startsWith(RedisConstants.SECKILL_BEGIN_KEY), anyString());
        verify(ops).setIfAbsent(startsWith(RedisConstants.SECKILL_END_KEY), anyString());
        verify(ops, never()).setIfAbsent(startsWith(RedisConstants.SECKILL_STOCK_KEY), anyString());
    }

    @Test
    void repairReturnsFailedWhenRedisThrows() {
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_BEGIN_KEY), anyString())).thenReturn(true);
        when(ops.setIfAbsent(startsWith(RedisConstants.SECKILL_END_KEY), anyString())).thenReturn(true);
        when(template.hasKey(STOCK_KEY)).thenThrow(new RuntimeException("redis down"));

        PublishResult result = initializer.repairStaticState(voucher(10));

        assertThat(result).isEqualTo(PublishResult.FAILED);
        verify(ops, never()).setIfAbsent(startsWith(RedisConstants.SECKILL_STOCK_KEY), anyString());
    }

    @Test
    void repairRejectsInvalidDataWithoutTouchingRedis() {
        assertThat(initializer.repairStaticState(null)).isEqualTo(PublishResult.FAILED);

        SeckillVoucher noVoucherId = voucher(10).setVoucherId(null);
        assertThat(initializer.repairStaticState(noVoucherId)).isEqualTo(PublishResult.FAILED);

        SeckillVoucher noEnd = voucher(10).setEndTime(null);
        assertThat(initializer.repairStaticState(noEnd)).isEqualTo(PublishResult.FAILED);

        verifyNoInteractions(template);
    }

    private SeckillVoucher voucher(int stock) {
        SeckillVoucher voucher = new SeckillVoucher();
        voucher.setVoucherId(VOUCHER_ID);
        voucher.setStock(stock);
        voucher.setBeginTime(LocalDateTime.now().minusHours(1));
        voucher.setEndTime(LocalDateTime.now().plusHours(1));
        return voucher;
    }
}
