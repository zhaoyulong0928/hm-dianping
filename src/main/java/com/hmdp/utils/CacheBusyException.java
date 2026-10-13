package com.hmdp.utils;

/**
 * 缓存重建繁忙降级异常：缓存未命中且短时间内无法获取重建锁时抛出，
 * 由全局异常处理器转换为统一的降级响应（如"商户信息加载中，请稍后重试"），
 * 避免请求线程无限等待或直接穿透到数据库。
 */
public class CacheBusyException extends RuntimeException {

    public CacheBusyException(String message) {
        super(message);
    }
}
