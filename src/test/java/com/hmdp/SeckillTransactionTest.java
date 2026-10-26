package com.hmdp;

import com.hmdp.dto.SeckillOrderMsg;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.ShopBloomFilter;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import javax.annotation.Resource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@Import(SeckillFaultTestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillTransactionTest extends SeckillIntegrationTestSupport {

    @Resource
    private SeckillMyBatisFaultInterceptor faultInterceptor;
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    @Test
    void shouldRollbackReturnedInsertBeforeFailingStockUpdateAndDeadLetter() throws InterruptedException {
        Long voucherId = prepareVoucher(1);
        long userId = 7_300_000_001L;
        long orderId = 9_100_000_000_001L;
        prepareOccupied(voucherId, userId, orderId);
        faultInterceptor.failStockUpdates(3);

        sendOrder(orderId, userId, voucherId);
        awaitDlqReady();
        awaitOrderQueueDrained();
        SeckillOrderMsg deadLetter = receiveOwnDlqMessage();
        SeckillTestSupport.awaitQueueDrained(
                System.getenv("SECKILL_TEST_RABBIT_VHOST"), MqConstants.SECKILL_DLX_QUEUE);

        assertThat(deadLetter).isNotNull();
        assertThat(deadLetter.getId()).isEqualTo(orderId);
        assertThat(deadLetter.getUserId()).isEqualTo(userId);
        assertThat(deadLetter.getVoucherId()).isEqualTo(voucherId);
        assertThat(faultInterceptor.insertReturned()).isEqualTo(3);
        assertThat(faultInterceptor.stockUpdateAttempts()).isEqualTo(3);
        assertThat(orderCount(voucherId)).isZero();
        assertThat(dbStock(voucherId)).isEqualTo(1);
        assertThat(redisStock(voucherId)).isZero();
        assertThat(occupied(voucherId, userId)).isTrue();
        System.out.printf("PHASE2_EVIDENCE case=tx-rollback-dlq runId=%s voucherId=%d orderId=%d insertReturned=%d updateAttempts=%d orders=%d dbStock=%d redisStock=%d occupied=%s dlqIdentity=true%n",
                runId, voucherId, orderId, faultInterceptor.insertReturned(), faultInterceptor.stockUpdateAttempts(),
                orderCount(voucherId), dbStock(voucherId), redisStock(voucherId), occupied(voucherId, userId));
    }
}
