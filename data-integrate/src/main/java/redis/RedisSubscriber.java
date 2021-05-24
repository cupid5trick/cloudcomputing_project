package redis;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import redis.RedisConf;

public class RedisSubscriber extends Thread {
    private String redisIp = RedisConf.getClusterIp();
    private int redisPort = RedisConf.getClusterPort();

    private JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, redisPort);


    private String channel;
    private JedisPubSub subscriber;

    public RedisSubscriber(String channel, JedisPubSub pubSub) {
        super("RedisSubscriber");
        this.channel = channel;
        this.subscriber = pubSub;
    }

    public RedisSubscriber(String channel, JedisPubSub pubSub, String redisIp, int redisPort) {
        super("RedisSubscriber");
        this.channel = channel;
        this.subscriber = pubSub;
        this.redisIp = redisIp;
        this.redisPort = redisPort;
    }

    @Override
    public void run() {
        System.out.println(String.format("subscribe redis, channel %s, thread will be blocked", channel));
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.subscribe(subscriber, channel);
        } catch (Exception e) {
            System.out.println(String.format("subsrcibe channel error, %s", e));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
