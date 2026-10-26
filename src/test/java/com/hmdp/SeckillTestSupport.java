package com.hmdp;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriUtils;

import javax.sql.DataSource;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;

final class SeckillTestSupport {

    private static final RestTemplate REST_TEMPLATE = new RestTemplate();

    private SeckillTestSupport() {
    }

    static void awaitQueueDrained(String vhost, String queue) throws InterruptedException {
        awaitQueueState(vhost, queue, 0, 0);
    }

    static void awaitQueueState(String vhost, String queue, int expectedReady,
                                int expectedUnacknowledged) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        int[] state = {-1, -1};
        do {
            state = queueState(vhost, queue);
            if (state[0] == expectedReady && state[1] == expectedUnacknowledged) {
                return;
            }
            Thread.sleep(100L);
        } while (System.currentTimeMillis() < deadline);
        throw new AssertionError("等待队列状态超时: queue=" + queue + ", ready=" + state[0]
                + ", unacknowledged=" + state[1] + ", expectedReady=" + expectedReady
                + ", expectedUnacknowledged=" + expectedUnacknowledged);
    }

    static int[] queueState(String vhost, String queue) {
        String host = System.getenv().getOrDefault("SECKILL_TEST_RABBIT_HOST", "127.0.0.1");
        String username = System.getenv().getOrDefault("SECKILL_TEST_RABBIT_USERNAME", "guest");
        String password = System.getenv().getOrDefault("SECKILL_TEST_RABBIT_PASSWORD", "guest");
        String encodedVhost = UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8);
        String encodedQueue = UriUtils.encodePathSegment(queue, StandardCharsets.UTF_8);
        URI uri = URI.create("http://" + host + ":15672/api/queues/" + encodedVhost + "/" + encodedQueue);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes(StandardCharsets.UTF_8)));
        HttpEntity<Void> request = new HttpEntity<>(headers);
        ResponseEntity<JsonNode> response = REST_TEMPLATE.exchange(uri, HttpMethod.GET, request, JsonNode.class);
        JsonNode body = response.getBody();
        if (body == null) {
            throw new AssertionError("队列状态响应为空: " + queue);
        }
        return new int[]{body.path("messages_ready").asInt(-1),
                body.path("messages_unacknowledged").asInt(-1)};
    }

    static String sessionSqlMode(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT @@SESSION.sql_mode")) {
            resultSet.next();
            return resultSet.getString(1);
        } catch (Exception e) {
            throw new AssertionError("读取测试数据源 SESSION sql_mode 失败", e);
        }
    }
}
