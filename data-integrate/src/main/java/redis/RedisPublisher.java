package redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import redis.RedisConf;

public class RedisPublisher {
    private final String redisIp = RedisConf.getClusterIp();
    private final int redisPort = RedisConf.getClusterPort();
    private JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, redisPort);
    private static Jedis jedis;

    public RedisPublisher() {
        jedis = jedisPool.getResource();
    }

    public RedisPublisher(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        jedis = this.jedisPool.getResource();
    }

    public void publish(String channel, String message) {
//        System.out.println("publish");
        jedis.publish(channel, message);
    }
}
