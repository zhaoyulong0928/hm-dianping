package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMsg;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RabbitTemplate rabbitTemplate;

    // 作用是加载lua脚本，用于秒杀，lua脚本中不能直接操作全局变量，只能操作局部变量，所以需要将参数赋值给局部变量
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 放在静态代码块里面，确保在类加载时加载lua脚本，避免在运行时加载lua脚本，导致性能问题
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.获取用户id
        Long userId = UserHolder.getUser().getId();
        // 2.生成订单id
        long orderId = redisIdWorker.nextId("order");
        // 3.执行lua脚本：原子完成 校验时间/库存/一人一单 + 扣库存 + 记录用户
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 4.判断结果是否为0
        if (r != 0) {
            if (r == 3) {
                return Result.fail("秒杀尚未开始");
            }
            if (r == 4) {
                return Result.fail("秒杀已经结束");
            }
            if (r == 5) {
                // 用户级限流触发：每用户每秒最多 1 次（seckill.lua 固定窗口），拦截并行/高频的脚本请求
                return Result.fail("操作过于频繁，请稍后再试");
            }
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 5.发送下单消息到 RabbitMQ
        SeckillOrderMsg msg = new SeckillOrderMsg(orderId, userId, voucherId);
        try {
            rabbitTemplate.convertAndSend(
                    MqConstants.SECKILL_ORDER_EXCHANGE,
                    MqConstants.SECKILL_ORDER_ROUTING_KEY,
                    msg,
                    new CorrelationData(String.valueOf(orderId)));
        } catch (AmqpException e) {
            // 同步发送失败（如连接异常），回滚 Redis 中的库存与一人一单记录，避免脏数据
            log.error("秒杀订单消息发送异常，回滚库存: orderId={}", orderId, e);
            rollbackSeckill(voucherId, userId);
            return Result.fail("下单失败，请重试");
        }
        // 6.返回订单id
        return Result.ok(orderId);
    }

    /**
     * MQ 消费者：并发消费落库。失败会由容器重试 N 次后进入死信队列。
     */
    @RabbitListener(queues = MqConstants.SECKILL_ORDER_QUEUE)
    @Transactional
    public void onOrderMessage(SeckillOrderMsg msg) {
        log.info("收到秒杀下单消息: {}", msg);
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(msg.getId());
        voucherOrder.setUserId(msg.getUserId());
        voucherOrder.setVoucherId(msg.getVoucherId());
        try {
            createVoucherOrder(voucherOrder);
        } catch (DuplicateKeyException e) {
            // 唯一索引兜底：重复消息直接忽略（幂等），正常确认不重试
            log.warn("重复下单消息，忽略: {}", msg);
        }
    }

    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 并发下同一用户的多条重复消息，未获取到锁的直接跳过（另一条会落库）
            log.warn("未获取到锁，跳过本次下单: userId={}", userId);
            return;
        }

        try {
            // 1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 1.1.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.warn("用户已购买过，忽略: userId={}, voucherId={}", userId, voucherId);
                return;
            }

            // 2.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                throw new IllegalStateException("库存不足");
            }

            // 3.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    /**
     * 回滚 Redis 中的库存与一人一单记录（用于消息发送失败补偿）
     */
    private void rollbackSeckill(Long voucherId, Long userId) {
        stringRedisTemplate.opsForValue().increment(RedisConstants.SECKILL_STOCK_KEY + voucherId, 1);
        stringRedisTemplate.opsForSet().remove("seckill:order:" + voucherId, userId.toString());
    }
}