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
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 秒杀并发一致性测试：
 * 1. 多用户并发抢有限库存 —— 验证不超卖、Redis 与 DB 账本一致。
 * 2. 同一(用户,券)重复消息 —— 验证删掉分布式锁后，唯一索引 + count 仍防住重复下单。
 *
 * 运行前提：本地需启动 MySQL / Redis / RabbitMQ（见 application.yaml 默认 localhost）。
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillConcurrencyTest {

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private IVoucherService voucherService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * 用 Mock 替换对账定时任务：@EnableScheduling 启动的 @Scheduled 线程是非 daemon 的，
     * 会让 surefire fork 的 JVM 在测试跑完后迟迟无法退出。替换后该 bean 不再注册调度任务。
     */
    @MockBean
    private SeckillReconciliationTask reconciliationTask;

    /** 记录本测试创建的券 id，供 @AfterEach 清理，保证测试可重复、不污染库 */
    private final List<Long> createdVoucherIds = new ArrayList<>();

    /**
     * 准备一张「进行中」的秒杀券：写 DB（tb_voucher / tb_seckill_voucher）并同步 Redis 库存/起止时间。
     */
    private Long prepareVoucher(int stock) {
        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle("并发测试券");
        voucher.setSubTitle("test");
        voucher.setRules("test");
        voucher.setPayValue(1L);
        voucher.setActualValue(1L);
        voucher.setType(1); // 1 = 秒杀券
        voucher.setStock(stock);
        voucher.setBeginTime(LocalDateTime.now().minusHours(1));
        voucher.setEndTime(LocalDateTime.now().plusHours(1));
        voucherService.addSeckillVoucher(voucher);
        Long vid = voucher.getId();
        createdVoucherIds.add(vid);
        return vid;
    }

    /**
     * 轮询等待该券的落库订单数达到预期（MQ 消费是异步的，不能请求一返回就断言）。
     * 超时或数量不对时由最后的断言报出真实值。
     */
    private void awaitOrderCount(Long voucherId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        int count;
        do {
            count = voucherOrderService.query().eq("voucher_id", voucherId).count();
            if (count == expected) {
                return;
            }
            Thread.sleep(200);
        } while (System.currentTimeMillis() < deadline);
        assertThat(count).as("等待订单数=" + expected + " 超时").isEqualTo(expected);
    }

    /**
     * 多用户并发抢有限库存：100 库存、1000 个不同用户各抢一次。
     * 期望：Lua 精确放行 100 次，DB 恰好落 100 单，Redis 与 DB 库存同步归零，无超卖。
     */
    @Test
    void shouldNotOversellUnderConcurrency() throws InterruptedException {
        int stock = 100;
        int users = 1000;
        Long vid = prepareVoucher(stock);

        // 线程数必须 >= 任务数：否则前 N 个任务在 start.await() 阻塞，后 N 个任务排不进线程池，
        // 永远无法 countDown ready，导致 ready.await() 死锁
        ExecutorService es = Executors.newFixedThreadPool(users);
        CountDownLatch ready = new CountDownLatch(users);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(users);
        AtomicInteger successCount = new AtomicInteger();

        for (int i = 0; i < users; i++) {
            final long userId = 1_000_000L + i;
            es.submit(() -> {
                UserDTO user = new UserDTO();
                user.setId(userId);
                UserHolder.saveUser(user);
                try {
                    ready.countDown();
                    start.await();
                    Result result = voucherOrderService.seckillVoucher(vid);
                    if (Boolean.TRUE.equals(result.getSuccess())) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    UserHolder.removeUser();
                    done.countDown();
                }
            });
        }

        ready.await(30, TimeUnit.SECONDS);
        start.countDown();
        done.await(60, TimeUnit.SECONDS);
        es.shutdown();

        // 等 MQ 全部消费落库后再断言
        awaitOrderCount(vid, stock);

        int orderCount = voucherOrderService.query().eq("voucher_id", vid).count();
        Integer dbStock = seckillVoucherService.getById(vid).getStock();
        Integer redisStock = getRedisStock(vid);

        assertThat(successCount.get()).as("Lua 放行（返回成功）次数应恰等于库存").isEqualTo(stock);
        assertThat(orderCount).as("DB 落单数，不得超卖").isEqualTo(stock);
        assertThat(dbStock).as("DB 库存归零且不为负").isEqualTo(0);
        assertThat(redisStock).as("Redis 剩余库存应与 DB 一致").isEqualTo(0);
    }

    /**
     * 删锁回归：同一(用户,券)出现多条重复消息时，仍只落 1 单、库存只扣 1 次。
     * 用 RabbitTemplate 直接投 5 条 (userId,voucherId) 相同、orderId 不同的消息，
     * 绕过 seckillVoucher 的前置限流/占坑，直接验证消费端「count 查重 + 唯一索引」兜底。
     */
    @Test
    void shouldOnlyCreateOneOrderWhenDuplicateMessages() throws InterruptedException {
        int stock = 10;
        Long vid = prepareVoucher(stock);
        long userId = 2_000_000L;

        // 模拟 Lua 已完成的「预扣」：Redis 库存 -1 + 占坑
        stringRedisTemplate.opsForValue().decrement(RedisConstants.SECKILL_STOCK_KEY + vid);
        stringRedisTemplate.opsForSet().add("seckill:order:" + vid, String.valueOf(userId));

        int duplicates = 5;
        for (int i = 0; i < duplicates; i++) {
            long orderId = 9_000_000_000_000L + i;
            SeckillOrderMsg msg = new SeckillOrderMsg(orderId, userId, vid);
            rabbitTemplate.convertAndSend(
                    MqConstants.SECKILL_ORDER_EXCHANGE,
                    MqConstants.SECKILL_ORDER_ROUTING_KEY,
                    msg,
                    new CorrelationData(String.valueOf(orderId)));
        }

        awaitOrderCount(vid, 1);

        int orderCount = voucherOrderService.query()
                .eq("voucher_id", vid).eq("user_id", userId).count();
        Integer dbStock = seckillVoucherService.getById(vid).getStock();
        Integer redisStock = getRedisStock(vid);

        assertThat(orderCount).as("重复消息只应落 1 单").isEqualTo(1);
        assertThat(dbStock).as("DB 库存只扣 1 次").isEqualTo(stock - 1);
        assertThat(redisStock).as("Redis 库存应与 DB 一致").isEqualTo(stock - 1);
    }

    private Integer getRedisStock(Long voucherId) {
        String value = stringRedisTemplate.opsForValue().get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        return value == null ? null : Integer.parseInt(value);
    }

    @AfterEach
    void cleanup() {
        for (Long vid : createdVoucherIds) {
            voucherOrderService.remove(new QueryWrapper<VoucherOrder>().eq("voucher_id", vid));
            seckillVoucherService.removeById(vid);
            voucherService.removeById(vid);
            stringRedisTemplate.delete(Arrays.asList(
                    RedisConstants.SECKILL_STOCK_KEY + vid,
                    RedisConstants.SECKILL_BEGIN_KEY + vid,
                    RedisConstants.SECKILL_END_KEY + vid,
                    "seckill:order:" + vid
            ));
        }
        createdVoucherIds.clear();
    }
}