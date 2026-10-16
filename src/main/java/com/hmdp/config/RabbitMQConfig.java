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

    // ============ 缓存删除：延迟队列(TTL) + 实际删除队列 + 死信队列 ============

    @Bean
    public DirectExchange cacheDeleteExchange() {
        return new DirectExchange(MqConstants.CACHE_DELETE_EXCHANGE, true, false);
    }

    /**
     * 延迟队列：消息在此停留 CACHE_DELETE_DELAY_MILLIS(500ms) 后过期，
     * 经死信投递到 cacheDeleteExchange，凭 CACHE_DELETE_ROUTING_KEY 转发给实际删除队列。
     * 相比原"本地线程池延迟删除"：消息持久化在 RabbitMQ，宕机不丢、失败可重试可追踪。
     */
    @Bean
    public Queue cacheDeleteDelayQueue() {
        // 持久化队列，并绑定死信交换机（消费失败重试耗尽后进入）
        Map<String, Object> args = new HashMap<>();
        // 消息过期时间（500ms）
        args.put("x-message-ttl", MqConstants.CACHE_DELETE_DELAY_MILLIS);
        // 死信交换机（cacheDeleteExchange）
        args.put("x-dead-letter-exchange", MqConstants.CACHE_DELETE_EXCHANGE);
        // 死信路由键（CACHE_DELETE_ROUTING_KEY）
        args.put("x-dead-letter-routing-key", MqConstants.CACHE_DELETE_ROUTING_KEY);
        // 消息持久化在 RabbitMQ
        return new Queue(MqConstants.CACHE_DELETE_DELAY_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding cacheDeleteDelayBinding() {
        return BindingBuilder.bind(cacheDeleteDelayQueue())// 这是延迟队列的绑定
                .to(cacheDeleteExchange())// 这个的作用是将延迟队列绑定到缓存删除交换机上
                .with(MqConstants.CACHE_DELETE_DELAY_ROUTING_KEY);

    }

    /**
     * 实际删除队列：消费者消费后执行 Redis 删除。
     * 删除是幂等操作，可安全重试；消费失败（抛异常）重试耗尽后进入缓存删除死信队列，供告警 / 人工补偿。
     */
    @Bean
    public Queue cacheDeleteQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", MqConstants.CACHE_DLX_EXCHANGE); // 死信交换机
        args.put("x-dead-letter-routing-key", MqConstants.CACHE_DLX_ROUTING_KEY);// 死信路由键
        return new Queue(MqConstants.CACHE_DELETE_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding cacheDeleteBinding() {
        return BindingBuilder.bind(cacheDeleteQueue())
                .to(cacheDeleteExchange())
                .with(MqConstants.CACHE_DELETE_ROUTING_KEY);
    }

    /**
     * 死信交换机：死信队列、JSON 转换器、重试与并发消费，为死信兜底提供保障
     */
    @Bean
    public DirectExchange cacheDlxExchange() {
        return new DirectExchange(MqConstants.CACHE_DLX_EXCHANGE, true, false);
    }

    @Bean
    public Queue cacheDlxQueue() {
        return new Queue(MqConstants.CACHE_DLX_QUEUE, true);
    }

    @Bean
    public Binding cacheDlxBinding() {
        return BindingBuilder.bind(cacheDlxQueue())
                .to(cacheDlxExchange())
                .with(MqConstants.CACHE_DLX_ROUTING_KEY);
    }

    // ============ 消息转换器：JSON ============

    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ============ RabbitTemplate：JSON + 发送方确认 + 失败回退 ============

    /*
     * 场景                         ConfirmCallback          ReturnCallback     你的处理
     * 成功路由到队列                  ack=true                   不触发           正常完成
     * 交换机不存在 / Broker 未确认    通常 ack=false               通常不触发      记录“消息未确认”
     * 交换机存在但无匹配队列           通常 ack=true               触发           记录“消息路由失败”
     * 消费者处理失败                 与此无关                     与此无关        需消费端重试、ACK、DLX 处理
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        // 开启 mandatory： mandatory=true 表示：若消息已经到达交换机，但交换机根据 routing key
        // 找不到任何匹配队列，RabbitMQ 不要静默丢弃，而是把消息退回给生产者。
        template.setMandatory(true);

        /*   发送方确认（publisher confirm）：broker 未确认时记录日志
         * • ack = true ：Broker 已确认收到并处理了发布请求
         * • ack = false ：Broker 未确认，可能是交换机不存在、连接异常等；你这里会记录错误日志
         * • correlationData ：消息的关联标识，用于定位到底是哪一条消息发送失败
         * • cause ：失败原因（若有）
         */
        // 发送方确认（publisher confirm）：broker 未确认时记录日志
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("消息未确认: correlationData={}, cause={}", correlationData, cause);
            }
        });
        // 消息无法路由到队列时触发（spring-amqp 2.2 使用 ReturnCallback）
        template.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> log.error(
                "消息路由失败: message={}, replyCode={}, replyText={}, exchange={}, routingKey={}",
                message, replyCode, replyText, exchange, routingKey));
        return template;
    }

    // ============ 重试拦截器：失败重试 N 次后拒绝不重回队列 → 进入死信队列 ============

    @Bean
    public RetryOperationsInterceptor seckillRetryInterceptor() {
        return RetryInterceptorBuilder
                .stateless()
                .maxAttempts(3)// 最大重试次数
                .backOffOptions(1000L, 2.0, 3000L) // 重试间隔（毫秒）
                .recoverer(new RejectAndDontRequeueRecoverer()) // 拒绝不重回队列
                .build(); // 构建重试拦截器
    }

    // ============ 监听容器工厂：并发消费 + 自动确认 + 重试拦截器 ，这个工厂对所有不指定`containerFactory` 的监听器生效。============

    @Bean("rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter messageConverter,
            RetryOperationsInterceptor seckillRetryInterceptor) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory); // · 连接工厂
        // 与生产者保持一致，使用 JSON 反序列化
        factory.setMessageConverter(messageConverter); // · 消息转换器
        // 自动确认：消息处理完成后自动确认，避免消息丢失
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        // 每个消费者一次预取几条，控制数据库压力
        factory.setPrefetchCount(10); // · 每个消费者一次预取几条
        factory.setConcurrentConsumers(4); // · 并发消费者数
        factory.setMaxConcurrentConsumers(8); // · 最大并发消费者数
        factory.setAdviceChain(seckillRetryInterceptor); // · 重试拦截器注册
        return factory;
    }
}