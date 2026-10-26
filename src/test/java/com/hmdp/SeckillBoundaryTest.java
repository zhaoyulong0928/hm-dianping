package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMsg;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.ShopBloomFilter;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** 仅验证 Service + Lua/Redis 准入边界，RabbitMQ 生产者被 mock，不属于落库全链路。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillBoundaryTest {

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private IVoucherService voucherService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Value("${seckill.test.run-id}")
    private String runId;

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
    void shouldRejectBeforeBeginTime() {
        Long voucherId = prepareVoucher(2, LocalDateTime.now().plusHours(1), LocalDateTime.now().plusHours(2));
        long userId = 7_200_000_001L;

        Result result = seckill(voucherId, userId);

        assertRejectedWithoutWrite(voucherId, userId, result, "秒杀尚未开始", 2);
        verifyNoInteractions(rabbitTemplate);
        printEvidence("not-started", voucherId, userId, result, 2);
    }

    @Test
    void shouldRejectAfterEndTime() {
        Long voucherId = prepareVoucher(2, LocalDateTime.now().minusHours(2), LocalDateTime.now().minusHours(1));
        long userId = 7_200_000_002L;

        Result result = seckill(voucherId, userId);

        assertRejectedWithoutWrite(voucherId, userId, result, "秒杀已经结束", 2);
        verifyNoInteractions(rabbitTemplate);
        printEvidence("already-ended", voucherId, userId, result, 2);
    }

    @Test
    void shouldRejectZeroStock() {
        Long voucherId = prepareVoucher(0, LocalDateTime.now().minusHours(1), LocalDateTime.now().plusHours(1));
        long userId = 7_200_000_003L;

        Result result = seckill(voucherId, userId);

        assertRejectedWithoutWrite(voucherId, userId, result, "库存不足", 0);
        verifyNoInteractions(rabbitTemplate);
        printEvidence("zero-stock", voucherId, userId, result, 0);
    }

    @Test
    void shouldAllowOnlyOneRequestForLastStock() {
        Long voucherId = prepareVoucher(1, LocalDateTime.now().minusHours(1), LocalDateTime.now().plusHours(1));
        long firstUser = 7_200_000_004L;
        long secondUser = 7_200_000_005L;

        Result first = seckill(voucherId, firstUser);
        Result second = seckill(voucherId, secondUser);

        assertThat(first.getSuccess()).isTrue();
        assertThat(first.getData()).isNotNull();
        assertRejectedWithoutWrite(voucherId, secondUser, second, "库存不足", 0);
        assertThat(isOccupied(voucherId, firstUser)).isTrue();
        verifyMessageSentOnce();
        System.out.printf("PHASE1_EVIDENCE case=last-stock runId=%s voucherId=%d firstUser=%d secondUser=%d firstSuccess=%s secondError=%s redisStock=%s%n",
                runId, voucherId, firstUser, secondUser, first.getSuccess(), second.getErrorMsg(), getRedisStock(voucherId));
    }

    @Test
    void shouldRejectDuplicatePurchaseWithoutSecondDeduction() throws InterruptedException {
        Long voucherId = prepareVoucher(2, LocalDateTime.now().minusHours(1), LocalDateTime.now().plusHours(1));
        long userId = 7_200_000_006L;

        Result first = seckill(voucherId, userId);
        Thread.sleep(1_100L);
        Result second = seckill(voucherId, userId);

        assertThat(first.getSuccess()).isTrue();
        assertThat(first.getData()).isNotNull();
        assertThat(second.getSuccess()).isFalse();
        assertThat(second.getErrorMsg()).isEqualTo("不能重复下单");
        assertThat(getRedisStock(voucherId)).isEqualTo(1);
        assertThat(isOccupied(voucherId, userId)).isTrue();
        verifyMessageSentOnce();
        System.out.printf("PHASE1_EVIDENCE case=duplicate-purchase runId=%s voucherId=%d userId=%d firstSuccess=%s secondError=%s redisStock=%s%n",
                runId, voucherId, userId, first.getSuccess(), second.getErrorMsg(), getRedisStock(voucherId));
    }

    private Long prepareVoucher(int stock, LocalDateTime beginTime, LocalDateTime endTime) {
        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle("phase1-boundary-" + runId);
        voucher.setSubTitle("phase1");
        voucher.setRules("dedicated test data only");
        voucher.setPayValue(1L);
        voucher.setActualValue(1L);
        voucher.setType(1);
        voucher.setStock(stock);
        voucher.setBeginTime(beginTime);
        voucher.setEndTime(endTime);
        voucherService.addSeckillVoucher(voucher);
        createdVoucherIds.add(voucher.getId());
        return voucher.getId();
    }

    private Result seckill(Long voucherId, long userId) {
        usedUserIds.add(userId);
        UserDTO user = new UserDTO();
        user.setId(userId);
        UserHolder.saveUser(user);
        try {
            return voucherOrderService.seckillVoucher(voucherId);
        } finally {
            UserHolder.removeUser();
        }
    }

    private void assertRejectedWithoutWrite(Long voucherId, long userId, Result result,
                                            String errorMessage, int expectedStock) {
        assertThat(result.getSuccess()).isFalse();
        assertThat(result.getErrorMsg()).isEqualTo(errorMessage);
        assertThat(getRedisStock(voucherId)).isEqualTo(expectedStock);
        assertThat(isOccupied(voucherId, userId)).isFalse();
    }

    private void verifyMessageSentOnce() {
        verify(rabbitTemplate).convertAndSend(
                eq(MqConstants.SECKILL_ORDER_EXCHANGE),
                eq(MqConstants.SECKILL_ORDER_ROUTING_KEY),
                any(SeckillOrderMsg.class),
                any(CorrelationData.class));
    }

    private Boolean isOccupied(Long voucherId, long userId) {
        return stringRedisTemplate.opsForHash().hasKey("seckill:order:" + voucherId, String.valueOf(userId));
    }

    private Integer getRedisStock(Long voucherId) {
        String value = stringRedisTemplate.opsForValue().get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        return value == null ? null : Integer.parseInt(value);
    }

    private void printEvidence(String testCase, Long voucherId, long userId, Result result, int stock) {
        System.out.printf("PHASE1_EVIDENCE case=%s runId=%s voucherId=%d userId=%d success=%s error=%s redisStock=%s occupied=%s%n",
                testCase, runId, voucherId, userId, result.getSuccess(), result.getErrorMsg(),
                getRedisStock(voucherId), isOccupied(voucherId, userId));
    }

    @AfterEach
    void cleanup() {
        for (Long voucherId : createdVoucherIds) {
            voucherOrderService.remove(new QueryWrapper<VoucherOrder>().eq("voucher_id", voucherId));
            seckillVoucherService.removeById(voucherId);
            voucherService.removeById(voucherId);
            stringRedisTemplate.delete(Arrays.asList(
                    RedisConstants.SECKILL_STOCK_KEY + voucherId,
                    RedisConstants.SECKILL_BEGIN_KEY + voucherId,
                    RedisConstants.SECKILL_END_KEY + voucherId,
                    "seckill:order:" + voucherId
            ));
        }
        for (Long userId : usedUserIds) {
            stringRedisTemplate.delete("seckill:freq:" + userId);
        }
        createdVoucherIds.clear();
        usedUserIds.clear();
    }
}
