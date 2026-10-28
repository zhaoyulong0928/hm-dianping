package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.task.SeckillActivityRedisInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillActivityRedisInitializer seckillActivityRedisInitializer;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 1.事务前提：必须在任何数据库写入之前确认，避免写完库才发现前提不成立
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("addSeckillVoucher 必须在 Spring 事务代理调用中执行");
        }

        // 2.业务参数校验：直接调用 Service 时也无法绕过
        validateForCreate(voucher);

        // 3.保存优惠券
        save(voucher);

        // 4.保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);

        // 5.数据库提交成功后，再发布 Redis 活动状态。
        // afterCommit 是本地回调，不是可靠投递：回调前进程崩溃仍可能缺少 Redis 状态。
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    seckillActivityRedisInitializer.publishNewActivity(seckillVoucher);
                } catch (Exception e) {
                    // 兜底：组件内部已捕获；此处确保已提交事务不因回调异常变成接口错误
                    log.error("[秒杀初始化] afterCommit 未预期异常: voucherId={}",
                            seckillVoucher.getVoucherId(), e);
                }
            }
        });
    }

    /**
     * 校验创建秒杀券的必要参数。
     * stock == 0 合法（现有测试以 0 表示售罄），负数非法。
     */
    private void validateForCreate(Voucher voucher) {
        if (voucher == null) {
            throw new IllegalArgumentException("优惠券信息不能为空");
        }
        if (voucher.getBeginTime() == null) {
            throw new IllegalArgumentException("秒杀开始时间不能为空");
        }
        if (voucher.getEndTime() == null) {
            throw new IllegalArgumentException("秒杀结束时间不能为空");
        }
        if (!voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            throw new IllegalArgumentException("秒杀开始时间必须早于结束时间");
        }
        if (voucher.getStock() == null) {
            throw new IllegalArgumentException("秒杀库存不能为空");
        }
        if (voucher.getStock() < 0) {
            throw new IllegalArgumentException("秒杀库存不能为负数");
        }
    }
}
