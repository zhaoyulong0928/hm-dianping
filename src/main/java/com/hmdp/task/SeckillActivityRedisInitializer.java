package com.hmdp.task;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.ZoneId;

/**
 * 秒杀活动 Redis 状态的首次发布与静态补齐。
 *
 * <p>边界：
 * <ul>
 *     <li>{@link #publishNewActivity(SeckillVoucher)} 只用于“新活动数据库提交后的首次发布”，
 *     不得用于历史活动恢复；缺失库存能否按数据库重建，本方法无法证明。</li>
 *     <li>{@link #repairStaticState(SeckillVoucher)} 只补静态时间键；库存缺失只返回
 *     {@link PublishResult#SKIPPED_UNSAFE}，绝不写库存。</li>
 *     <li>时间键先写、库存键最后写；时间阶段失败则不发布库存。</li>
 *     <li>一律 setIfAbsent，不覆盖可能已被预扣的库存。</li>
 * </ul>
 */
@Slf4j
@Component
public class SeckillActivityRedisInitializer {

    public enum PublishResult {
        /** 本次实际写入了至少一个 key */
        PUBLISHED,
        /** 所需 key 已存在，幂等跳过 */
        ALREADY_EXISTS,
        /** 库存缺失，但本方案禁止自动重建 */
        SKIPPED_UNSAFE,
        /** 必要写入失败（异常或命令返回 null） */
        FAILED
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 新活动首次发布：时间键 → 库存键。
     * 仅供数据库提交后的发布链路调用，不得用于历史活动恢复。
     */
    public PublishResult publishNewActivity(SeckillVoucher voucher) {
        if (voucher == null) {
            log.error("[秒杀初始化] voucher 为空，拒绝发布");
            return PublishResult.FAILED;
        }
        Long vid = voucher.getVoucherId();
        String stage = "validate";
        try {
            if (!isPublishable(voucher)) {
                log.error("[秒杀初始化] 参数不满足发布条件: voucherId={}", vid);
                return PublishResult.FAILED;
            }

            stage = "writeTimeKeys";
            PublishResult timeResult = writeTimeKeys(voucher);
            if (timeResult == PublishResult.FAILED) {
                log.error("[秒杀初始化] 时间键写入失败，未发布库存: voucherId={}", vid);
                return PublishResult.FAILED;
            }

            stage = "writeStock";
            PublishResult stockResult = writeStock(voucher);
            if (stockResult == PublishResult.FAILED) {
                log.error("[秒杀初始化] 库存发布失败，活动未就绪: voucherId={}", vid);
                return PublishResult.FAILED;
            }

            PublishResult overall = (timeResult == PublishResult.PUBLISHED
                    || stockResult == PublishResult.PUBLISHED)
                    ? PublishResult.PUBLISHED
                    : PublishResult.ALREADY_EXISTS;
            if (overall == PublishResult.PUBLISHED) {
                log.info("[秒杀初始化] 新活动发布完成: voucherId={}", vid);
            } else {
                // 幂等跳过，不记为错误
                log.debug("[秒杀初始化] 新活动已发布，跳过: voucherId={}", vid);
            }
            return overall;
        } catch (Exception e) {
            // 统一异常边界：只捕获 Exception，不吞 Error
            log.error("[秒杀初始化] 阶段={} 异常: voucherId={}", stage, vid, e);
            return PublishResult.FAILED;
        }
    }

    /**
     * 对账使用：只补静态时间键，库存缺失只告警。
     * 不得调用 {@link #publishNewActivity(SeckillVoucher)} 间接写库存。
     */
    public PublishResult repairStaticState(SeckillVoucher voucher) {
        if (voucher == null) {
            log.error("[秒杀对账] voucher 为空，跳过补齐");
            return PublishResult.FAILED;
        }
        Long vid = voucher.getVoucherId();
        String stage = "validate";
        try {
            if (!isStaticStateRepairable(voucher)) {
                log.error("[秒杀对账] 参数不满足补齐条件: voucherId={}", vid);
                return PublishResult.FAILED;
            }

            stage = "writeTimeKeys";
            PublishResult timeResult = writeTimeKeys(voucher);
            if (timeResult == PublishResult.FAILED) {
                log.error("[秒杀对账] 时间键补齐失败: voucherId={}", vid);
                return PublishResult.FAILED;
            }

            stage = "checkStock";
            Boolean exists = stringRedisTemplate.hasKey(RedisConstants.SECKILL_STOCK_KEY + vid);
            if (exists == null) {
                log.error("[秒杀对账] 库存存在性检查返回 null: voucherId={}", vid);
                return PublishResult.FAILED;
            }
            if (exists) {
                if (timeResult == PublishResult.PUBLISHED) {
                    log.warn("[秒杀对账] 已补齐时间键: vid={}", vid);
                } else {
                    log.debug("[秒杀对账] 状态已完整，跳过: vid={}", vid);
                }
                return timeResult;
            }
            // 有意不写库存：数据库库存不含在途预扣，无法证明按数据库重建是安全的
            log.error("[秒杀对账][告警] 库存 key 缺失，禁止自动重灌，需人工恢复: vid={}", vid);
            return PublishResult.SKIPPED_UNSAFE;
        } catch (Exception e) {
            log.error("[秒杀对账] 阶段={} 异常: voucherId={}", stage, vid, e);
            return PublishResult.FAILED;
        }
    }

    private boolean isPublishable(SeckillVoucher voucher) {
        return voucher.getVoucherId() != null
                && voucher.getStock() != null
                && voucher.getStock() >= 0
                && voucher.getBeginTime() != null
                && voucher.getEndTime() != null
                && voucher.getBeginTime().isBefore(voucher.getEndTime());
    }

    private boolean isStaticStateRepairable(SeckillVoucher voucher) {
        return voucher.getVoucherId() != null
                && voucher.getBeginTime() != null
                && voucher.getEndTime() != null
                && voucher.getBeginTime().isBefore(voucher.getEndTime());
    }

    private PublishResult writeTimeKeys(SeckillVoucher voucher) {
        Long vid = voucher.getVoucherId();
        Boolean begin = stringRedisTemplate.opsForValue().setIfAbsent(
                RedisConstants.SECKILL_BEGIN_KEY + vid,
                String.valueOf(voucher.getBeginTime()
                        .atZone(ZoneId.systemDefault()).toEpochSecond()));
        Boolean end = stringRedisTemplate.opsForValue().setIfAbsent(
                RedisConstants.SECKILL_END_KEY + vid,
                String.valueOf(voucher.getEndTime()
                        .atZone(ZoneId.systemDefault()).toEpochSecond()));
        if (begin == null || end == null) {
            return PublishResult.FAILED;
        }
        return (begin || end) ? PublishResult.PUBLISHED : PublishResult.ALREADY_EXISTS;
    }

    private PublishResult writeStock(SeckillVoucher voucher) {
        Boolean written = stringRedisTemplate.opsForValue().setIfAbsent(
                RedisConstants.SECKILL_STOCK_KEY + voucher.getVoucherId(),
                String.valueOf(voucher.getStock()));
        if (written == null) {
            return PublishResult.FAILED;
        }
        return written ? PublishResult.PUBLISHED : PublishResult.ALREADY_EXISTS;
    }
}
