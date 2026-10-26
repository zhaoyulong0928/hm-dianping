package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /**
     * 回滚 Redis 中的秒杀预扣（库存 +1、移除一人一单占坑），幂等，可被多个失败路径安全共用
     *
     * @param rollbackToken 回滚资格令牌：订单维度用 "order:{orderId}"，对账维度用 "recon:{vid}:{uid}"，
     *                      同一 token 只能回滚一次（Lua 幂等闸门）
     * @param voucherId     优惠券 id
     * @param userId        用户 id
     * @param expectedOrderId 期望回滚的订单 id；仅当前占坑仍属于该订单时才允许回滚
     * @return true = 本次执行了回滚；false = 占坑不存在/不属于该订单或本 token 已回滚过
     */
    boolean rollbackSeckill(String rollbackToken, Long voucherId, Long userId, String expectedOrderId);
}
