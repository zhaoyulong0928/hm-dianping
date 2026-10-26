package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.task.SeckillReconciliationTask;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.ShopBloomFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("seckill-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SeckillHttpTest {

    private static final long USER_ID = 7_100_000_001L;

    @LocalServerPort
    private int port;
    @Resource
    private TestRestTemplate restTemplate;
    @Resource
    private IVoucherService voucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Value("${seckill.test.run-id}")
    private String runId;

    /** 只关闭与主链路无关的定时对账和未使用的 Redisson；HTTP、Lua、MQ、DB 均为真实组件。 */
    @MockBean
    private SeckillReconciliationTask reconciliationTask;
    @MockBean
    private RedissonClient redissonClient;
    @MockBean
    private ShopBloomFilter shopBloomFilter;

    private final List<Long> createdVoucherIds = new ArrayList<>();
    private final String token = "phase1-http-" + UUID.randomUUID();

    @Test
    void shouldCreateOrderThroughRealHttpAndMq() throws InterruptedException {
        Long voucherId = prepareVoucher(1);
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        Map<String, String> userMap = new HashMap<>();
        userMap.put("id", String.valueOf(USER_ID));
        userMap.put("nickName", "phase1-http");
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);

        HttpHeaders headers = new HttpHeaders();
        headers.set("authorization", token);
        ResponseEntity<Result> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/voucher-order/seckill/" + voucherId,
                new HttpEntity<>(headers), Result.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Result body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getSuccess()).isTrue();
        assertThat(body.getData()).isNotNull();
        long orderId = Long.parseLong(String.valueOf(body.getData()));

        awaitOrderCount(voucherId, 1);
        int orderCount = voucherOrderService.query().eq("voucher_id", voucherId).count();
        VoucherOrder order = voucherOrderService.query()
                .eq("voucher_id", voucherId).eq("user_id", USER_ID).one();
        Integer dbStock = seckillVoucherService.getById(voucherId).getStock();
        Integer redisStock = getRedisStock(voucherId);

        assertThat(orderCount).isEqualTo(1);
        assertThat(order.getId()).isEqualTo(orderId);
        assertThat(dbStock).isZero();
        assertThat(redisStock).isZero();
        System.out.printf("PHASE1_EVIDENCE case=http-full runId=%s voucherId=%d userId=%d orderId=%d http=%d orders=%d dbStock=%d redisStock=%d%n",
                runId, voucherId, USER_ID, orderId, response.getStatusCodeValue(), orderCount, dbStock, redisStock);
    }

    private Long prepareVoucher(int stock) {
        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle("phase1-http-" + runId);
        voucher.setSubTitle("phase1");
        voucher.setRules("dedicated test data only");
        voucher.setPayValue(1L);
        voucher.setActualValue(1L);
        voucher.setType(1);
        voucher.setStock(stock);
        voucher.setBeginTime(LocalDateTime.now().minusHours(1));
        voucher.setEndTime(LocalDateTime.now().plusHours(1));
        voucherService.addSeckillVoucher(voucher);
        createdVoucherIds.add(voucher.getId());
        return voucher.getId();
    }

    private void awaitOrderCount(Long voucherId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        int count;
        do {
            count = voucherOrderService.query().eq("voucher_id", voucherId).count();
            if (count == expected) {
                return;
            }
            Thread.sleep(200);
        } while (System.currentTimeMillis() < deadline);
        assertThat(count).isEqualTo(expected);
    }

    private Integer getRedisStock(Long voucherId) {
        String value = stringRedisTemplate.opsForValue().get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        return value == null ? null : Integer.parseInt(value);
    }

    @AfterEach
    void cleanup() {
        for (Long voucherId : createdVoucherIds) {
            voucherOrderService.remove(new QueryWrapper<VoucherOrder>().eq("voucher_id", voucherId));
            seckillVoucherService.removeById(voucherId);
            voucherService.removeById(voucherId);
            List<String> keys = new ArrayList<>(Arrays.asList(
                    RedisConstants.SECKILL_STOCK_KEY + voucherId,
                    RedisConstants.SECKILL_BEGIN_KEY + voucherId,
                    RedisConstants.SECKILL_END_KEY + voucherId,
                    "seckill:order:" + voucherId,
                    "seckill:freq:" + USER_ID,
                    RedisConstants.LOGIN_USER_KEY + token
            ));
            stringRedisTemplate.delete(keys);
        }
        createdVoucherIds.clear();
    }
}
