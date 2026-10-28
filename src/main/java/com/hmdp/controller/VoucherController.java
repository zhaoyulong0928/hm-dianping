package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher")
public class VoucherController {

    @Resource
    private IVoucherService voucherService;

    /**
     * 新增秒杀券
     * @param voucher 优惠券信息，包含秒杀信息
     * @return 优惠券id
     */
    @PostMapping("seckill")
    public Result addSeckillVoucher(@RequestBody Voucher voucher) {
        // 友好提示：Service 内部也会再做一次业务校验，防止直接调用绕过
        if (voucher.getBeginTime() == null || voucher.getEndTime() == null) {
            return Result.fail("秒杀开始时间和结束时间不能为空");
        }
        if (!voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            return Result.fail("秒杀开始时间必须早于结束时间");
        }
        if (voucher.getStock() == null || voucher.getStock() < 0) {
            return Result.fail("秒杀库存不能为空且不能为负数");
        }
        voucherService.addSeckillVoucher(voucher);
        // 语义：数据库活动创建成功；Redis 初始化在事务提交后执行，不代表活动一定已可售
        return Result.ok(voucher.getId());
    }

    /**
     * 新增普通券
     * @param voucher 优惠券信息
     * @return 优惠券id
     */
    @PostMapping
    public Result addVoucher(@RequestBody Voucher voucher) {
        voucherService.save(voucher);
        return Result.ok(voucher.getId());
    }


    /**
     * 查询店铺的优惠券列表
     * @param shopId 店铺id
     * @return 优惠券列表
     */
    @GetMapping("/list/{shopId}")
    public Result queryVoucherOfShop(@PathVariable("shopId") Long shopId) {
       return voucherService.queryVoucherOfShop(shopId);
    }
}
