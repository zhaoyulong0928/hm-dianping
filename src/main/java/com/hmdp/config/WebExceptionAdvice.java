package com.hmdp.config;

import com.hmdp.dto.Result;
import com.hmdp.utils.CacheBusyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class WebExceptionAdvice {

    /** 缓存重建繁忙的降级响应：把降级文案原样返回给前端，不走"服务器异常"兜底 */
    @ExceptionHandler(CacheBusyException.class)
    public Result handleCacheBusyException(CacheBusyException e) {
        log.warn("缓存重建繁忙，返回降级响应：{}", e.getMessage());
        return Result.fail(e.getMessage());
    }

    @ExceptionHandler(RuntimeException.class)
    public Result handleRuntimeException(RuntimeException e) {
        log.error(e.toString(), e);
        return Result.fail("服务器异常");
    }
}
