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
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.annotation.Resource;
import java.util.Arrays;
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

    // 加载幂等回滚脚本：SETNX 标记 + 库存回增 + 移除一人一单记录，单脚本原子完成
    private static final DefaultRedisScript<Long> ROLLBACK_SCRIPT;

    /** 回滚标记存活时间（秒）：远大于任何补偿路径的重试周期，过期自动清理，无需人工回收 */
    private static final long ROLLBACK_MARKER_TTL_SECONDS = 24 * 60 * 60L;

    // 放在静态代码块里面，确保在类加载时加载lua脚本，避免在运行时加载lua脚本，导致性能问题
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
        ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        ROLLBACK_SCRIPT.setLocation(new ClassPathResource("rollbackSeckill.lua"));
        ROLLBACK_SCRIPT.setResultType(Long.class);
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
            rollbackSeckill("order:" + orderId, voucherId, userId);
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
            // 落库终检：消费期间占坑被补偿（极端场景：消费卡顿超过对账宽限期，
            // 被对账任务误判漂移先行补偿）。本单已无对应预扣，若提交会变成
            // "有单无占坑"的幽灵订单 → 手动标记回滚（订单 + 扣库存一并撤销），
            // 消息正常确认：资格已退还用户可重新下单，重试本消息无意义
            if (!isSeckillOccupied(msg.getVoucherId(), msg.getUserId())) {
                log.warn("落库期间占坑已被补偿，撤销本单: {}", msg);
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            }
        } catch (DuplicateKeyException e) {
            // 唯一索引兜底：重复消息直接忽略（幂等），正常确认不重试。
            // save 已前置于扣库存，此时事务内未发生任何写操作，提交无副作用
            log.warn("重复下单消息，忽略: {}", msg);
        } catch (IllegalStateException e) {
            // 库存不足：终态失败（Redis/DB 账本漂移所致），重试无意义，就地补偿，
            // 死信队列因此只承接基础设施故障，退化为"有消息即告警"的健康仪表盘。
            // 关键：catch 住不抛后事务默认提交，而 save() 的 INSERT 还在事务里，
            // 必须手动标记回滚，否则提交出"有订单没扣库存"的脏数据
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            log.error("库存不足终态失败，补偿回滚 Redis: voucherId={}, userId={}",
                    msg.getVoucherId(), msg.getUserId());
            rollbackSeckill("order:" + msg.getId(), msg.getVoucherId(), msg.getUserId());
        }
    }

    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 资格预检：占坑记录已被某条补偿路径回滚（如对账任务误补偿），
        // 说明本消息对应的预扣资格已退还用户（可重新下单），
        // 此时落库会制造"有单无占坑 + 库存漂移"的幽灵订单，直接跳过，正常确认消息
        if (!isSeckillOccupied(voucherId, userId)) {
            log.warn("占坑已被补偿回滚，跳过落库: userId={}, voucherId={}", userId, voucherId);
            return;
        }
        // 创建锁对象（锁粒度 = 用户 + 券：只有同一(用户,券)的重复消息才互斥；
        // 同一用户抢不同券的合法并发互不干扰，避免"抢锁失败即跳过"误杀正常订单）
        RLock redisLock = redissonClient.getLock("lock:order:" + userId + ":" + voucherId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 并发下同一(用户,券)的重复消息，未获取到锁的直接跳过（另一条会落库，唯一索引兜底）
            log.warn("未获取到锁，跳过本次下单: userId={}, voucherId={}", userId, voucherId);
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

            // 2.创建订单（前置于扣库存：并发重复消息在扣库存之前就会被唯一索引拦截，
            // 即使异常被外层 catch 吞掉后事务照常提交，事务内也未发生任何写操作，不会出现"扣库存无订单"的脏提交）
            save(voucherOrder);

            // 3.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败：抛出异常回滚整个事务（含已插入的订单），重试耗尽后进死信队列
                throw new IllegalStateException("库存不足");
            }
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    /**
     * 回滚 Redis 中的库存与一人一单记录（幂等，可被多个失败路径安全共用）
     *
     * Lua 三重防线：SISMEMBER 业务守卫（用户已不占坑 = 已被其他路径回滚，跳过）
     * → SET NX EX 幂等闸门（同一 rollbackToken 只回滚一次）
     * → INCRBY + SREM 原子回滚（无"回滚一半"中间态）
     *
     * rollbackToken 取值约定：订单维度 "order:{orderId}"（生产端同步失败 / 消费端终态补偿 /
     * 未来的死信消费者），对账维度 "recon:{vid}:{uid}"（定时对账任务）
     */
    @Override
    public boolean rollbackSeckill(String rollbackToken, Long voucherId, Long userId) {
        Long result = stringRedisTemplate.execute(
                ROLLBACK_SCRIPT,
                Arrays.asList(
                        RedisConstants.SECKILL_ROLLBACK_MARKER_KEY + rollbackToken, // 回滚标记
                        RedisConstants.SECKILL_STOCK_KEY + voucherId,              // 库存
                        "seckill:order:" + voucherId                               // 一人一单记录
                ),
                userId.toString(), String.valueOf(ROLLBACK_MARKER_TTL_SECONDS)
        );
        boolean rolledBack = result != null && result == 1L;
        if (!rolledBack) {
            log.warn("回滚未执行(无占坑或本token已回滚): token={}, voucherId={}, userId={}",
                    rollbackToken, voucherId, userId);
        }
        return rolledBack;
    }

    /**
     * 检查用户是否仍持有该券的一人一单占坑记录
     * （与 seckill.lua 的 SADD / rollbackSeckill.lua 的 SREM 对应）
     */
    private boolean isSeckillOccupied(Long voucherId, Long userId) {
        return Boolean.TRUE.equals(stringRedisTemplate.opsForSet()
                .isMember("seckill:order:" + voucherId, userId.toString()));
    }
}