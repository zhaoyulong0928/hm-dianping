package com.hmdp.task;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 *   基线断言 —— 以 DB 券列表驱动，校验「Redis 剩余库存 == DB 剩余库存」，只告警不修复
 *   Phase 1 扫描 —— SCAN seckill:order:* 取占坑用户，按券一次 IN 查询 DB 得「有单用户集」
 *   Phase 2 裁决 —— 两阶段确认（suspect-then-confirm）：
 *     · DB 有单 → 账实相符，清嫌疑
 *     · 无单且首见 → 仅记录嫌疑 key（value=首见时间戳），不动手
 *     · 无单且嫌疑已满宽限期 → 确认漂移，告警 + 幂等补偿
 *
 * 设计要点：
 *   · 宽限期显式判断（elapsed >= GRACE_MS）而非依赖调度间隔，手动触发/多实例也安全；
 *     常规在途（消费 + 重试退避）仅秒级 << 60s；极端在途（如 DB 行锁等待 50s×3 次 ≈ 153s）
 *     `50s` 是 MySQL InnoDB 的`innodb_lock_wait_timeout` 默认值—— 
 *     当一个事务去 UPDATE 时撞上了别的行锁，会最多傻等 50 秒才报"锁等待超时" 。
 *     可能超过宽限期，由消费端占坑双检兜底（见 VoucherOrderServiceImpl 落库前后 HEXISTS 校验）
 *   · 补偿令牌用 "recon:{vid}:{uid}" 区分来源；实际回滚还必须通过 Hash 中的字符串 orderId 归属校验
 *   · suspect key 追加 orderId，旧订单的更新/清理不会覆盖或删除同用户新订单的嫌疑
 *   · 死信非空时整轮让位为保守策略：一条无关死信会停摆全部对账。
 *     生产演进：解析死信消息体，仅隔离相关 voucherId/userId 的对账
 *   · 单实例假设：订单归属校验 + rollbackToken 双闸门保证补偿幂等，
 *     仅重复扫描浪费；多实例部署时应引入 ShedLock
 */
@Slf4j
@Component
public class SeckillReconciliationTask {

    /** 一人一单占坑 Hash key 前缀（field=userId，value=字符串 orderId） */
    private static final String ORDER_KEY_PREFIX = "seckill:order:";

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
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)//每 60 秒执行一次，应用启动后 60 秒后开始，避免还没就绪的窗口
    /**
     * 秒杀定时对账任务（anti-entropy）：兜住所有"Redis 已占坑、DB 永无订单"的静默漂移
     */
    public void reconcile() {
        //作用：确保"占坑 + 无单"只可能是漂移而非在途
        if (!guardsPassed()) {
            return;
        }
        // 基线断言（账本级）：以 DB 券列表驱动 —— 占坑集合已清空 / key 丢失的券
        // （漂移已自愈、Redis 数据丢失）不在下方 SCAN 范围内，只有 DB 能枚举出它们
        //对 DB 里的 每一张秒杀券 ，比较`Redis 库存` 和`DB 库存` 是否相等。
        // 它的覆盖范围天然是“全部券”，不依赖任何 Redis key 是否存在。
        for (SeckillVoucher voucher : seckillVoucherService.list()) {
            try {
                assertBaseline(voucher.getVoucherId(), voucher.getStock());
            } catch (Exception e) {
                // 单券断言异常不拖垮整轮（如 Redis/DB 抖动）
                log.error("[秒杀对账] 基线断言异常: vid={}", voucher.getVoucherId(), e);
            }
        }
        Set<String> orderSetKeys = scanOrderKeys();
        for (String orderKey : orderSetKeys) {
            try {
                reconcileVoucher(orderKey);
            } catch (Exception e) {
                // 单券异常不拖垮整轮（如 Redis/DB 抖动），嫌疑 key 依赖 TTL 自清理
                log.error("[秒杀对账] 单券对账异常: orderKey={}", orderKey, e);
            }
        }
    }

    /**
     * Phase 0 守卫：确保"占坑 + 无单"只可能是漂移而非在途
     */
    private boolean guardsPassed() {
        QueueInformation orderQueue = amqpAdmin.getQueueInfo(MqConstants.SECKILL_ORDER_QUEUE);
        if (orderQueue == null || orderQueue.getConsumerCount() == 0) {
            //队列不存在或者消费者全挂了，消息全堆在队列里，此刻“占坑无单”遍地都是，但都是在途
            log.warn("[秒杀对账] 消费者不在线，消息可能在队列堆积，本轮跳过");
            return false;
        }
        if (orderQueue.getMessageCount() > 0) {
            //队列里哪怕还有 1 条消息，它可能就是那个“占坑无单”用户的消息
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
     * `KEYS` 是 O(N) 阻塞命令 ——扫描期间 Redis 主线程不干别的，
     * 生产上大 keyspace 一把 KEYS 能卡死整个 Redis。`SCAN` 用游标分批（`count(200)` 每批建议扫 200 个 slot），随时让路给正常请求
     * Redis 是单线程模型，`KEYS` 执行期间别的命令一个都插不进来。
     * 对一个秒杀系统来说， 在活动高峰期执行 KEYS 等于主动制造一次全站卡顿 。SCAN 就是官方为这个场景设计的替代品。
     */
    private Set<String> scanOrderKeys() {
        return stringRedisTemplate.execute((RedisCallback<Set<String>>) connection -> {
            Set<String> keys = new HashSet<>();
            try (Cursor<byte[]> cursor = connection.scan(
                    ScanOptions.scanOptions().match(ORDER_KEY_PREFIX + "*").count(200).build())) {
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
    private void reconcileVoucher(String orderKey) {
        //
        Long voucherId;
        try {
            voucherId = Long.parseLong(orderKey.substring(ORDER_KEY_PREFIX.length()));
        } catch (NumberFormatException e) {
            log.warn("[秒杀对账] 无法从 key 解析券 id，跳过: {}", orderKey);
            return;
        }
        // 占坑 Hash：field=userId，value=字符串 orderId；保留 DB 是否已成功落单的判断
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(orderKey);
        if (entries.isEmpty()) {
            return;
        }
        Map<Long, String> reservationOrderIds = new HashMap<>();
        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            reservationOrderIds.put(Long.parseLong(entry.getKey().toString()), entry.getValue().toString());
        }
        List<Long> memberIds = new ArrayList<>(reservationOrderIds.keySet());
        Set<Long> orderedUserIds = voucherOrderService.query()
                .eq("voucher_id", voucherId)
                .in("user_id", memberIds)
                .list()
                .stream()
                .map(VoucherOrder::getUserId)
                .collect(Collectors.toSet());

        for (Map.Entry<Long, String> reservation : reservationOrderIds.entrySet()) {
            Long userId = reservation.getKey();
            String orderId = reservation.getValue();
            if (orderedUserIds.contains(userId)) {
                // 只清理当前订单的嫌疑，不能误删同用户其他订单的嫌疑
                stringRedisTemplate.delete(suspectKey(voucherId, userId, orderId));
                continue;
            }
            judgeSuspect(voucherId, userId, orderId);
        }
    }

    /**
     * 基线断言：稳态下 Redis 剩余库存应等于 DB 剩余库存（守卫已保证队列无在途，
     * 一笔单要么两边各扣一次、要么被补偿两边都不扣，故两数必然相等）。
     * 不等 = 有绕过正常链路的改动（人工改库 / Redis 数据丢失 / 旧值残留）。
     * 只告警不自动修复：无法程序化裁决信 Redis 还是信 DB（两个方向修复相反），
     * 人工按 SOP 重灌（SET seckill:stock:{vid} = DB 库存值）后下一轮自然安静。
     * redisStock == null（key 不存在）同样告警：Redis 数据丢失也属基线漂移
     */
    private void assertBaseline(Long voucherId, Integer dbStock) {
        String redisStock = stringRedisTemplate.opsForValue()
                .get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        if (redisStock == null || Long.parseLong(redisStock) != dbStock) {
            log.error("[秒杀对账][告警] 库存基线漂移: vid={}, redis={}, db={}, 请人工裁决后重灌",
                    voucherId, redisStock, dbStock);
        }
    }

    /**
     * Phase 3：两阶段裁决单个"占坑无单"用户
     */
    private void judgeSuspect(Long voucherId, Long userId, String orderId) {
        // 嫌疑按订单隔离，旧订单的更新和清理不会影响同用户的新订单
        String suspectKey = suspectKey(voucherId, userId, orderId);
        //作用：获取嫌疑 key 对应的值（记录首次见时间）
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
            // 只有当前占坑仍属于该 orderId 才允许补偿；token 继续负责同路径幂等
            voucherOrderService.rollbackSeckill("recon:" + voucherId + ":" + userId,
                    voucherId, userId, orderId);
            // suspect key 含 orderId，清理旧订单嫌疑不会删除新订单嫌疑
            stringRedisTemplate.delete(suspectKey);
        } catch (Exception e) {
            log.error("[秒杀对账] 补偿执行失败，保留嫌疑待下轮重试: vid={}, uid={}", voucherId, userId, e);
        }
    }

    /** 嫌疑 key 绑定订单，旧订单 suspect 只按 TTL 自然清理，不覆盖新订单 */
    private String suspectKey(Long voucherId, Long userId, String orderId) {
        return RedisConstants.SECKILL_RECON_SUSPECT_KEY + voucherId + ":" + userId + ":" + orderId;
    }
}
