package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 缓存删除消息（投递到 RabbitMQ 延迟队列，用于缓存"第二次删除"的可靠兜底）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheDeleteMsg implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待删除的缓存 key（如 cache:shop:1）
     */
    private String key;
}