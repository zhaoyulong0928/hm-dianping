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
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillCompensationRaceTest extends SeckillIntegrationTestSupport {

    @MockBean
    private RabbitTemplate rabbitTemplate;
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    @Test
    void shouldNotLetLateOldCompensationCancelNewPurchase() {
        Long voucherId = prepareVoucher(1);
        long userId = 7_300_000_005L;
        long oldOrderId = 9_200_000_000_001L;
        String oldOrderToken = "order:" + oldOrderId;
        String oldReconToken = "recon:" + voucherId + ":" + userId;
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + oldOrderToken);
        trackRedisKey(RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + oldReconToken);

        // 固定顺序：旧请求预扣 -> 旧请求首次补偿 -> 同用户重新抢购 -> 旧补偿迟到重试。
        prepareOccupied(voucherId, userId, oldOrderId);
        boolean firstCompensation = voucherOrderService.rollbackSeckill(
                oldOrderToken, voucherId, userId, String.valueOf(oldOrderId));
        assertThat(firstCompensation).isTrue();
        assertThat(redisStock(voucherId)).isEqualTo(1);
        assertThat(occupied(voucherId, userId)).isFalse();

        UserDTO user = new UserDTO();
        user.setId(userId);
        UserHolder.saveUser(user);
        Result newPurchase;
        try {
            newPurchase = voucherOrderService.seckillVoucher(voucherId);
        } finally {
            UserHolder.removeUser();
        }
        ArgumentCaptor<SeckillOrderMsg> captor = ArgumentCaptor.forClass(SeckillOrderMsg.class);
        verify(rabbitTemplate).convertAndSend(eq(MqConstants.SECKILL_ORDER_EXCHANGE),
                eq(MqConstants.SECKILL_ORDER_ROUTING_KEY), captor.capture(), any(CorrelationData.class));
        long newOrderId = captor.getValue().getId();
        assertThat(newPurchase.getSuccess()).isTrue();
        assertThat(redisStock(voucherId)).isZero();
        assertThat(occupied(voucherId, userId)).isTrue();

        boolean sameTokenLate = voucherOrderService.rollbackSeckill(
                oldOrderToken, voucherId, userId, String.valueOf(oldOrderId));
        assertThat(sameTokenLate).isFalse();
        assertThat(redisStock(voucherId)).isZero();
        assertThat(occupied(voucherId, userId)).isTrue();

        // 另一旧补偿路径使用尚未消耗的 recon token，业务守卫应保护新订单的占坑。
        boolean otherTokenLate = voucherOrderService.rollbackSeckill(
                oldReconToken, voucherId, userId, String.valueOf(oldOrderId));
        System.out.printf("PHASE2_EVIDENCE case=stale-compensation-race runId=%s voucherId=%d userId=%d oldOrderId=%d newOrderId=%d firstCompensation=%s sameTokenLate=%s otherTokenLate=%s stockAfterLate=%s occupiedAfterLate=%s%n",
                runId, voucherId, userId, oldOrderId, newOrderId, firstCompensation,
                sameTokenLate, otherTokenLate, redisStock(voucherId), occupied(voucherId, userId));
        assertThat(otherTokenLate).as("旧补偿不得撤销重新抢购后的新占坑").isFalse();
        assertThat(redisStock(voucherId)).as("旧补偿不得恢复新请求已扣库存").isZero();
        assertThat(occupied(voucherId, userId)).as("旧补偿不得移除新请求占坑").isTrue();
    }
}
