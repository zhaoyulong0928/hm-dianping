package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.SeckillOrderMsg;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisConstants;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

abstract class SeckillIntegrationTestSupport {

    @Resource
    protected IVoucherOrderService voucherOrderService;
    @Resource
    protected IVoucherService voucherService;
    @Resource
    protected ISeckillVoucherService seckillVoucherService;
    @Resource
    protected StringRedisTemplate stringRedisTemplate;
    @Resource
    protected RabbitTemplate rabbitTemplate;
    @Resource
    protected ObjectMapper objectMapper;
    @Resource
    protected DataSource dataSource;
    @Value("${seckill.test.run-id}")
    protected String runId;

    private final List<Long> createdVoucherIds = new ArrayList<>();
    private final Set<Long> usedUserIds = new HashSet<>();
    private final Set<String> extraRedisKeys = new HashSet<>();

    @BeforeEach
    void verifyIsolatedQueuesAndSampleSqlMode() throws InterruptedException {
        System.out.println("PHASE2_TEST_SESSION_SQL_MODE=" + SeckillTestSupport.sessionSqlMode(dataSource));
        String vhost = System.getenv("SECKILL_TEST_RABBIT_VHOST");
        SeckillTestSupport.awaitQueueDrained(vhost, MqConstants.SECKILL_ORDER_QUEUE);
        SeckillTestSupport.awaitQueueDrained(vhost, MqConstants.SECKILL_DLX_QUEUE);
        assertThat(SeckillTestSupport.queueState(vhost, MqConstants.SECKILL_ORDER_QUEUE))
                .as("每组开始前订单队列必须无残留").containsExactly(0, 0);
        assertThat(SeckillTestSupport.queueState(vhost, MqConstants.SECKILL_DLX_QUEUE))
                .as("每组开始前死信队列必须无残留").containsExactly(0, 0);
    }

    protected Long prepareVoucher(int stock) {
        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle("phase2-" + runId);
        voucher.setSubTitle("phase2");
        voucher.setRules("dedicated test data only");
        voucher.setPayValue(1L);
        voucher.setActualValue(1L);
        voucher.setType(1);
        voucher.setStock(stock);
        voucher.setBeginTime(LocalDateTime.now().minusHours(1));
        voucher.setEndTime(LocalDateTime.now().plusHours(1));
        voucherService.addSeckillVoucher(voucher);
        createdVoucherIds.add(voucher.getId());
        return voucher.getId();
    }

    protected void prepareOccupied(Long voucherId, long userId, long orderId) {
        usedUserIds.add(userId);
        stringRedisTemplate.opsForValue().decrement(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        stringRedisTemplate.opsForHash().put("seckill:order:" + voucherId,
                String.valueOf(userId), String.valueOf(orderId));
    }

    protected void sendOrder(long orderId, long userId, Long voucherId) {
        SeckillOrderMsg msg = new SeckillOrderMsg(orderId, userId, voucherId);
        rabbitTemplate.convertAndSend(MqConstants.SECKILL_ORDER_EXCHANGE,
                MqConstants.SECKILL_ORDER_ROUTING_KEY, msg,
                new CorrelationData(String.valueOf(orderId)));
    }

    protected SeckillOrderMsg receiveOwnDlqMessage() {
        return rabbitTemplate.execute(channel -> {
            GetResponse response = channel.basicGet(MqConstants.SECKILL_DLX_QUEUE, false);
            if (response == null) {
                return null;
            }
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            SeckillOrderMsg order = objectMapper.readValue(response.getBody(), SeckillOrderMsg.class);
            if (order != null && createdVoucherIds.contains(order.getVoucherId())) {
                channel.basicAck(deliveryTag, false);
            } else {
                channel.basicNack(deliveryTag, false, true);
            }
            return order;
        });
    }

    protected void awaitOrderQueueDrained() throws InterruptedException {
        SeckillTestSupport.awaitQueueDrained(
                System.getenv("SECKILL_TEST_RABBIT_VHOST"), MqConstants.SECKILL_ORDER_QUEUE);
    }

    protected void awaitDlqReady() throws InterruptedException {
        SeckillTestSupport.awaitQueueState(
                System.getenv("SECKILL_TEST_RABBIT_VHOST"), MqConstants.SECKILL_DLX_QUEUE, 1, 0);
    }

    protected int orderCount(Long voucherId) {
        return voucherOrderService.query().eq("voucher_id", voucherId).count();
    }

    protected void awaitOrderCount(Long voucherId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        int count;
        do {
            count = orderCount(voucherId);
            if (count == expected) {
                return;
            }
            Thread.sleep(100L);
        } while (System.currentTimeMillis() < deadline);
        assertThat(count).as("等待订单数达到 " + expected).isEqualTo(expected);
    }

    protected Integer dbStock(Long voucherId) {
        return seckillVoucherService.getById(voucherId).getStock();
    }

    protected Integer redisStock(Long voucherId) {
        String value = stringRedisTemplate.opsForValue().get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        return value == null ? null : Integer.parseInt(value);
    }

    protected boolean occupied(Long voucherId, long userId) {
        return Boolean.TRUE.equals(stringRedisTemplate.opsForHash()
                .hasKey("seckill:order:" + voucherId, String.valueOf(userId)));
    }

    protected void trackRedisKey(String key) {
        extraRedisKeys.add(key);
    }

    @AfterEach
    void cleanupCreatedResources() throws InterruptedException {
        // 即使断言失败，也不能与在途消息竞争删除；超时则保留数据并报告清理异常。
        awaitOrderQueueDrained();
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
        if (!extraRedisKeys.isEmpty()) {
            stringRedisTemplate.delete(extraRedisKeys);
        }
        createdVoucherIds.clear();
        usedUserIds.clear();
        extraRedisKeys.clear();
    }
}
