package com.hmdp.task;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 秒杀定时对账任务（anti-entropy）：兜住所有"Redis 已占坑、DB 永无订单"的静默漂移
 *
 * 覆盖的丢失来源：confirm 丢失（半开连接 / 进程重启蒸发）、nack/return 仅打日志的残余、
 * 未来任何未知路径。发现延迟为分钟级，换取对在途消息零误伤。
 *
 * 单轮流程：
 *   Phase 0 守卫 —— 消费者不在线 / 队列有在途消息 / 死信未清空（人工优先），任一命中本轮跳过
 *   Phase 1 扫描 —— SCAN seckill:order:* 取占坑用户，按券一次 IN 查询 DB 得「有单用户集」
 *   Phase 2 裁决 —— 两阶段确认（suspect-then-confirm）：
 *     · DB 有单 → 账实相符，清嫌疑
 *     · 无单且首见 → 仅记录嫌疑 key（value=首见时间戳），不动手
 *     · 无单且嫌疑已满宽限期 → 确认漂移，告警 + 幂等补偿
 *
 * 设计要点：
 *   · 宽限期显式判断（elapsed >= GRACE_MS）而非依赖调度间隔，手动触发/多实例也安全；
 *     在途上限 = 消费(毫秒级) + 重试3次(退避1s+2s) ≈ 10s << 60s（6 倍余量）
 *   · 补偿令牌用 "recon:{vid}:{uid}"：同一(用户,券)同时最多一单在途（lua SISMEMBER 拦截），
 *     二元组是安全代理键；与订单维度令牌 "order:{orderId}" 天然区分来源
 *   · 死信非空时整轮让位为保守策略：一条无关死信会停摆全部对账。
 *     生产演进：解析死信消息体，仅隔离相关 voucherId/userId 的对账
 *   · 单实例假设：补偿幂等（SISMEMBER + rollbackToken 双闸门）保证多实例下正确性不受损，
 *     仅重复扫描浪费；多实例部署时应引入 ShedLock
 */
@Slf4j
@Component
public class SeckillReconciliationTask {

    /** 一人一单占坑集合 key 前缀（与 seckill.lua 保持一致） */
    private static final String ORDER_SET_PREFIX = "seckill:order:";

    /** 两阶段裁决宽限期（毫秒）：首次发现无单后至少等待此时长才确认漂移 */
    private static final long GRACE_MS = 60_000L;

    /** 嫌疑 key 存活时间（秒）：漏扫/中断的嫌疑自动褪色，不残留 */
    private static final long SUSPECT_TTL_SECONDS = 1800L;

    @Resource
    private AmqpAdmin amqpAdmin;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IVoucherOrderService voucherOrderService;

    @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
    public void reconcile() {
        if (!guardsPassed()) {
            return;
        }
        Set<String> orderSetKeys = scanOrderSetKeys();
        for (String setKey : orderSetKeys) {
            try {
                reconcileVoucher(setKey);
            } catch (Exception e) {
                // 单券异常不拖垮整轮（如 Redis/DB 抖动），嫌疑 key 依赖 TTL 自清理
                log.error("[秒杀对账] 单券对账异常: setKey={}", setKey, e);
            }
        }
    }

    /**
     * Phase 0 守卫：确保"占坑 + 无单"只可能是漂移而非在途
     */
    private boolean guardsPassed() {
        QueueInformation orderQueue = amqpAdmin.getQueueInfo(MqConstants.SECKILL_ORDER_QUEUE);
        if (orderQueue == null || orderQueue.getConsumerCount() == 0) {
            log.warn("[秒杀对账] 消费者不在线，消息可能在队列堆积，本轮跳过");
            return false;
        }
        if (orderQueue.getMessageCount() > 0) {
            log.info("[秒杀对账] 秒杀队列存在 {} 条在途消息，本轮跳过", orderQueue.getMessageCount());
            return false;
        }
        QueueInformation dlxQueue = amqpAdmin.getQueueInfo(MqConstants.SECKILL_DLX_QUEUE);
        if (dlxQueue != null && dlxQueue.getMessageCount() > 0) {
            // 保守策略：死信=未裁决业务，人工处理优先，避免自动补偿与死信重放打架；
            // 死信清空后对账自动恢复。生产演进：按 vid/uid 维度隔离，而非整轮让位
            log.error("[秒杀对账][告警] 死信队列存在 {} 条消息待人工处理，本轮对账让位", dlxQueue.getMessageCount());
            return false;
        }
        return true;
    }

    /**
     * SCAN 一人一单占坑集合 key（不用 KEYS，避免阻塞 Redis 主线程）
     */
    private Set<String> scanOrderSetKeys() {
        return stringRedisTemplate.execute((RedisCallback<Set<String>>) connection -> {
            Set<String> keys = new HashSet<>();
            try (Cursor<byte[]> cursor = connection.scan(
                    ScanOptions.scanOptions().match(ORDER_SET_PREFIX + "*").count(200).build())) {
                while (cursor.hasNext()) {
                    keys.add(new String(cursor.next(), StandardCharsets.UTF_8));
                }
            }
            return keys;
        });
    }

    /**
     * Phase 1 + Phase 2：对单个券的占坑集合做比对与裁决
     */
    private void reconcileVoucher(String setKey) {
        Long voucherId;
        try {
            voucherId = Long.parseLong(setKey.substring(ORDER_SET_PREFIX.length()));
        } catch (NumberFormatException e) {
            log.warn("[秒杀对账] 无法从 key 解析券 id，跳过: {}", setKey);
            return;
        }
        Set<String> members = stringRedisTemplate.opsForSet().members(setKey);
        if (members == null || members.isEmpty()) {
            return;
        }
        List<Long> memberIds = members.stream().map(Long::parseLong).collect(Collectors.toList());
        // 每券一次 IN 查询（唯一索引 uk_user_voucher 点查），得「已落单用户集」
        Set<Long> orderedUserIds = voucherOrderService.query()
                .eq("voucher_id", voucherId)
                .in("user_id", memberIds)
                .list()
                .stream()
                .map(VoucherOrder::getUserId)
                .collect(Collectors.toSet());

        for (Long userId : memberIds) {
            if (orderedUserIds.contains(userId)) {
                // 账实相符；顺手清掉可能的旧嫌疑（虚惊收尾）
                stringRedisTemplate.delete(suspectKey(voucherId, userId));
                continue;
            }
            judgeSuspect(voucherId, userId);
        }
    }

    /**
     * Phase 2：两阶段裁决单个"占坑无单"用户
     */
    private void judgeSuspect(Long voucherId, Long userId) {
        String suspectKey = suspectKey(voucherId, userId);
        String firstSeen = stringRedisTemplate.opsForValue().get(suspectKey);
        long now = System.currentTimeMillis();

        if (firstSeen == null) {
            // 首见：只记录不动手（消息可能在途：消费/重试最长约 10s）
            stringRedisTemplate.opsForValue().set(suspectKey, String.valueOf(now),
                    Duration.ofSeconds(SUSPECT_TTL_SECONDS));
            log.info("[秒杀对账] 标记嫌疑: vid={}, uid={}, 等待下轮复查", voucherId, userId);
            return;
        }

        long elapsed = now - Long.parseLong(firstSeen);
        if (elapsed < GRACE_MS) {
            // 未满宽限期，继续等
            return;
        }

        // 确认漂移：占坑 + 持续无单（超过宽限期）+ 队列/死信皆空 → 消息已蒸发
        log.error("[秒杀对账][告警] 确认漂移: vid={}, uid={}, 首见于 {}ms 前，执行补偿",
                voucherId, userId, elapsed);
        try {
            // 幂等补偿：rollbackToken 用对账维度合成令牌；
            // 若用户已被其他路径回滚，SISMEMBER 守卫会拦截（返回 false），同样视为已了结
            voucherOrderService.rollbackSeckill("recon:" + voucherId + ":" + userId, voucherId, userId);
            // 补偿调用成功返回（含被幂等拦截）后清理嫌疑；若抛异常则不清理，下轮重试
            stringRedisTemplate.delete(suspectKey);
        } catch (Exception e) {
            log.error("[秒杀对账] 补偿执行失败，保留嫌疑待下轮重试: vid={}, uid={}", voucherId, userId, e);
        }
    }

    private String suspectKey(Long voucherId, Long userId) {
        return RedisConstants.SECKILL_RECON_SUSPECT_KEY + voucherId + ":" + userId;
    }
}
