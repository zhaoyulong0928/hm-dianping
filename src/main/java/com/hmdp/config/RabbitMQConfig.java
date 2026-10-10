package com.hmdp.config;

import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ 配置：交换机、队列、死信队列、JSON 转换器、重试与并发消费
 */
@Slf4j
@Configuration
public class RabbitMQConfig {

    // ============ 秒杀订单交换机 / 队列 / 绑定 ============

    @Bean
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(MqConstants.SECKILL_ORDER_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillOrderQueue() {
        // 持久化队列，并绑定死信交换机（消费失败重试耗尽后进入）
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", MqConstants.SECKILL_DLX_EXCHANGE);
        args.put("x-dead-letter-routing-key", MqConstants.SECKILL_DLX_ROUTING_KEY);
        return new Queue(MqConstants.SECKILL_ORDER_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding seckillOrderBinding() {
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(MqConstants.SECKILL_ORDER_ROUTING_KEY);
    }

    // ============ 死信交换机 / 队列 / 绑定 ============

    @Bean
    public DirectExchange seckillDlxExchange() {
        return new DirectExchange(MqConstants.SECKILL_DLX_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillDlxQueue() {
        return new Queue(MqConstants.SECKILL_DLX_QUEUE, true);
    }

    @Bean
    public Binding seckillDlxBinding() {
        return BindingBuilder.bind(seckillDlxQueue())
                .to(seckillDlxExchange())
                .with(MqConstants.SECKILL_DLX_ROUTING_KEY);
    }

    // ============ 消息转换器：JSON ============

    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ============ RabbitTemplate：JSON + 发送方确认 + 失败回退 ============

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        // 开启 mandatory：消息无法路由到队列时触发 ReturnsCallback，避免静默丢失
        template.setMandatory(true);
        // 发送方确认（publisher confirm）：broker 未确认时记录日志
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("消息未确认: correlationData={}, cause={}", correlationData, cause);
            }
        });
        // 消息无法路由到队列时触发（spring-amqp 2.2 使用 ReturnCallback）
        template.setReturnCallback((message, replyCode, replyText, exchange, routingKey) ->
                log.error("消息路由失败: message={}, replyCode={}, replyText={}, exchange={}, routingKey={}",
                        message, replyCode, replyText, exchange, routingKey));
        return template;
    }

    // ============ 重试拦截器：失败重试 N 次后拒绝不重回队列 → 进入死信队列 ============

    @Bean
    public RetryOperationsInterceptor seckillRetryInterceptor() {
        return RetryInterceptorBuilder.stateless()
                .maxAttempts(3)
                .backOffOptions(1000L, 2.0, 3000L)
                .recoverer(new RejectAndDontRequeueRecoverer())
                .build();
    }

    // ============ 监听容器工厂：并发消费 + 自动确认 + 重试拦截器 ============

    @Bean("rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter messageConverter, RetryOperationsInterceptor seckillRetryInterceptor) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        // 与生产者保持一致，使用 JSON 反序列化
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        // 每个消费者一次预取几条，控制数据库压力
        factory.setPrefetchCount(10);
        factory.setConcurrentConsumers(4);
        factory.setMaxConcurrentConsumers(8);
        factory.setAdviceChain(seckillRetryInterceptor);
        return factory;
    }
}