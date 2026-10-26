package com.hmdp;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMsg;
import com.hmdp.dto.UserDTO;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.ShopBloomFilter;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillCompensationTest extends SeckillIntegrationTestSupport {

    @MockBean
    private RabbitTemplate rabbitTemplate;
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    @Test
    void shouldRollbackExactlyOnceWhenProducerSendFails() {
        Long voucherId = prepareVoucher(1);
        long userId = 7_300_000_004L;
        doThrow(new AmqpException("PHASE2_SEND_FAILURE")).when(rabbitTemplate).convertAndSend(
                eq(MqConstants.SECKILL_ORDER_EXCHANGE), eq(MqConstants.SECKILL_ORDER_ROUTING_KEY),
                any(SeckillOrderMsg.class), any(CorrelationData.class));
        UserDTO user = new UserDTO();
        user.setId(userId);
        UserHolder.saveUser(user);
        Result result;
        try {
            result = voucherOrderService.seckillVoucher(voucherId);
        } finally {
            UserHolder.removeUser();
        }

        ArgumentCaptor<SeckillOrderMsg> captor = ArgumentCaptor.forClass(SeckillOrderMsg.class);
        verify(rabbitTemplate).convertAndSend(eq(MqConstants.SECKILL_ORDER_EXCHANGE),
                eq(MqConstants.SECKILL_ORDER_ROUTING_KEY), captor.capture(), any(CorrelationData.class));
        long orderId = captor.getValue().getId();
        String orderToken = "order:" + orderId;
        String reconToken = "recon:" + voucherId + ":" + userId;
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + orderToken);
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + reconToken);

        assertThat(result.getSuccess()).isFalse();
        assertThat(result.getErrorMsg()).isEqualTo("下单失败，请重试");
        assertThat(redisStock(voucherId)).isEqualTo(1);
        assertThat(occupied(voucherId, userId)).isFalse();
        boolean sameTokenRepeat = voucherOrderService.rollbackSeckill(
                orderToken, voucherId, userId, String.valueOf(orderId));
        boolean otherTokenRepeat = voucherOrderService.rollbackSeckill(
                reconToken, voucherId, userId, String.valueOf(orderId));
        assertThat(sameTokenRepeat).isFalse();
        assertThat(otherTokenRepeat).isFalse();
        assertThat(redisStock(voucherId)).isEqualTo(1);
        assertThat(occupied(voucherId, userId)).isFalse();
        System.out.printf("PHASE2_EVIDENCE case=producer-compensation runId=%s voucherId=%d userId=%d orderId=%d sendError=%s sameTokenRepeat=%s otherTokenRepeat=%s stock=%d occupied=%s%n",
                runId, voucherId, userId, orderId, result.getErrorMsg(), sameTokenRepeat,
                otherTokenRepeat, redisStock(voucherId), occupied(voucherId, userId));
    }
}
