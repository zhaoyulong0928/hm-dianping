package com.hmdp;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.task.SeckillActivityRedisInitializer;
import com.hmdp.task.SeckillActivityRedisInitializer.PublishResult;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.MqConstants;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 纯单元测试：验证对账单券失败不会中断后续券。
 * 不依赖 MySQL / Redis / RabbitMQ。
 */
class SeckillReconciliationTaskIsolationTest {

    @Test
    void singleVoucherFailureDoesNotStopFollowingVouchers() {
        SeckillVoucher first = new SeckillVoucher().setVoucherId(1L).setStock(10);
        SeckillVoucher second = new SeckillVoucher().setVoucherId(2L).setStock(10);

        ISeckillVoucherService voucherService = mock(ISeckillVoucherService.class);
        SeckillActivityRedisInitializer initializer = mock(SeckillActivityRedisInitializer.class);
        AmqpAdmin amqpAdmin = mock(AmqpAdmin.class);

        when(voucherService.list()).thenReturn(Arrays.asList(first, second));
        when(initializer.repairStaticState(first)).thenThrow(new RuntimeException("redis down"));
        when(initializer.repairStaticState(second)).thenReturn(PublishResult.ALREADY_EXISTS);
        // 守卫拿不到队列信息 → 本轮在补齐后结束，不进入基线/扫描
        when(amqpAdmin.getQueueInfo(anyString())).thenReturn(null);

        SeckillReconciliationTask task = new SeckillReconciliationTask();
        ReflectionTestUtils.setField(task, "seckillVoucherService", voucherService);
        ReflectionTestUtils.setField(task, "seckillActivityRedisInitializer", initializer);
        ReflectionTestUtils.setField(task, "amqpAdmin", amqpAdmin);

        task.reconcile();

        verify(initializer).repairStaticState(first);
        verify(initializer).repairStaticState(second);
        verify(amqpAdmin).getQueueInfo(MqConstants.SECKILL_ORDER_QUEUE);
    }
}
