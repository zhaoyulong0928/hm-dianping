package com.hmdp;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;

import java.util.concurrent.atomic.AtomicInteger;

/** 仅注入到测试 SqlSessionFactory；真实 INSERT 返回后计数，再按预设次数拒绝库存 UPDATE。 */
@Intercepts(@Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}))
public class SeckillMyBatisFaultInterceptor implements Interceptor {

    private static final String ORDER_INSERT = "com.hmdp.mapper.VoucherOrderMapper.insert";
    private static final String STOCK_UPDATE = "com.hmdp.mapper.SeckillVoucherMapper.update";

    private final AtomicInteger failuresRemaining = new AtomicInteger();
    private final AtomicInteger insertReturned = new AtomicInteger();
    private final AtomicInteger stockUpdateAttempts = new AtomicInteger();

    public void failStockUpdates(int failures) {
        insertReturned.set(0);
        stockUpdateAttempts.set(0);
        failuresRemaining.set(failures);
    }

    public int insertReturned() {
        return insertReturned.get();
    }

    public int stockUpdateAttempts() {
        return stockUpdateAttempts.get();
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement statement = (MappedStatement) invocation.getArgs()[0];
        if (ORDER_INSERT.equals(statement.getId())) {
            Object result = invocation.proceed();
            insertReturned.incrementAndGet();
            return result;
        }
        if (STOCK_UPDATE.equals(statement.getId())) {
            stockUpdateAttempts.incrementAndGet();
            int remaining = failuresRemaining.get();
            if (remaining > 0 && failuresRemaining.compareAndSet(remaining, remaining - 1)) {
                throw new RuntimeException("PHASE2_INJECT_STOCK_UPDATE_FAILURE");
            }
        }
        return invocation.proceed();
    }
}
