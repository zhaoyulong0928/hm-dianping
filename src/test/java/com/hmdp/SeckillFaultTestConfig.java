package com.hmdp;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class SeckillFaultTestConfig {

    @Bean
    public SeckillMyBatisFaultInterceptor seckillMyBatisFaultInterceptor() {
        return new SeckillMyBatisFaultInterceptor();
    }
}
