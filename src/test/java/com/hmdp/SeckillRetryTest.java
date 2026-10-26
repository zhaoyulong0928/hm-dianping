package com.hmdp;

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
class SeckillRetryTest extends SeckillIntegrationTestSupport {

    @Resource
    private SeckillMyBatisFaultInterceptor faultInterceptor;
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    @Test
    void shouldCountActualRetriesAndSucceedOnThirdAttempt() throws InterruptedException {
        Long voucherId = prepareVoucher(1);
        long userId = 7_300_000_002L;
        long orderId = 9_100_000_000_002L;
        prepareOccupied(voucherId, userId, orderId);
        faultInterceptor.failStockUpdates(2);

        sendOrder(orderId, userId, voucherId);
        awaitOrderCount(voucherId, 1);
        awaitOrderQueueDrained();

        assertThat(faultInterceptor.stockUpdateAttempts()).isEqualTo(3);
        assertThat(orderCount(voucherId)).isEqualTo(1);
        assertThat(dbStock(voucherId)).isZero();
        assertThat(redisStock(voucherId)).isZero();
        assertThat(occupied(voucherId, userId)).isTrue();
        assertThat(SeckillTestSupport.queueState(
                System.getenv("SECKILL_TEST_RABBIT_VHOST"), MqConstants.SECKILL_DLX_QUEUE))
                .containsExactly(0, 0);
        System.out.printf("PHASE2_EVIDENCE case=retry-success runId=%s voucherId=%d orderId=%d attempts=%d orders=%d dbStock=%d redisStock=%d dlq=0%n",
                runId, voucherId, orderId, faultInterceptor.stockUpdateAttempts(), orderCount(voucherId),
                dbStock(voucherId), redisStock(voucherId));
    }
}
