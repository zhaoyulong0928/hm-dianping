package com.hmdp.mq;

import com.hmdp.dto.CacheDeleteMsg;
import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 缓存删除消费者：执行"延迟双删"中的第二次删除兜底。
 * <p>
 * 相比原实现（本地线程池 500ms 后删一次）：消息持久化在 RabbitMQ，进程宕机不丢；
 * 删除失败（抛异常）走全局重试拦截器（3 次）后进入 cacheDlxQueue，供告警 / 人工补偿。
 * 删除 key 是幂等操作，重复投递 / 重复消费无副作用。
 */
@Slf4j
@Component
public class CacheDeleteListener {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @RabbitListener(queues = MqConstants.CACHE_DELETE_QUEUE)
    public void onCacheDelete(CacheDeleteMsg msg) {
        Boolean deleted = stringRedisTemplate.delete(msg.getKey());
        log.info("缓存二次删除执行完成: key={}, deleted={}", msg.getKey(), deleted);
    }
}