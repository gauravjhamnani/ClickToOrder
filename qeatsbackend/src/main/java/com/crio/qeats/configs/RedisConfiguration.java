
package com.crio.qeats.configs;

import javax.annotation.PostConstruct;

import lombok.Data;
import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Log4j2
@Component
@Data
@Primary
public class RedisConfiguration {

  // TODO: CRIO_TASK_MODULE_REDIS
  // The Jedis client for Redis goes through some initialization steps before you can
  // start using it as a cache.
  // Objective:
  // Some methods are empty or partially filled. Make it into a working implementation.
  public static final String redisHost = "localhost";

  // Amount of time after which the redis entries should expire.
  public static final int REDIS_ENTRY_EXPIRY_IN_SECONDS = 3600;

  // TIP(MODULE_RABBITMQ): RabbitMQ related configs.
  public static final String EXCHANGE_NAME = "rabbitmq-exchange";
  public static final String QUEUE_NAME = "rabbitmq-queue";
  public static final String ROUTING_KEY = "qeats.postorder";


  private int redisPort;
  private JedisPool jedisPool;


  @Value("${spring.redis.port}")
  public void setRedisPort(int port) {
    System.out.println("setting up redis port to " + port);
    redisPort = port;
  }

  /**
   * Initializes the cache to be used in the code.
   * TIP: Look in the direction of `JedisPool`.
   */
  @PostConstruct
  public void initCache() {
    this.jedisPool = new JedisPool(poolConfig(), redisHost, this.redisPort);
  }


  private JedisPoolConfig poolConfig() {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxIdle(0);
    return jedisPoolConfig;
  }

  /**
   * Checks is cache is intiailized and available. TIP: This would generally mean
   * checking via {@link JedisPool}
   * 
   * @return true / false if cache is available or not.
   */
  public boolean isCacheAvailable() {
    if (jedisPool != null) {
      System.out.println("cache is available");
      return true;
    } 
    return false;
  }

  /**
   * Destroy the cache.
   * TIP: This is useful if cache is stale or while performing tests.
   */
  public void destroyCache() {
    System.out.println("Destroying JedisPool");
    this.jedisPool.getResource().flushAll();
    // this.jedisPool.destroy();
  }

}

