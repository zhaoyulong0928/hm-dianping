package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 秒杀订单消息（投递到 RabbitMQ 的消息体）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMsg implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 订单id
     */
    private Long id;

    /**
     * 用户id
     */
    private Long userId;

    /**
     * 优惠券id
     */
    private Long voucherId;
}