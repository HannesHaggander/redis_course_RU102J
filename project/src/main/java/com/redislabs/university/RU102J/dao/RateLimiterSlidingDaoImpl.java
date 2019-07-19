package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.time.LocalDateTime;
import java.util.UUID;

public class RateLimiterSlidingDaoImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long maxHits;
    private final int seconds;
    private final UUID id;

    public RateLimiterSlidingDaoImpl(final JedisPool jedisPool, final int seconds, final long maxHits) {
        this.jedisPool = jedisPool;
        this.maxHits = maxHits;
        this.seconds = seconds;
        this.id = UUID.randomUUID();
    }

    @Override
    public void hit(String name) throws RateLimitExceededException {
        try(Jedis jedis = jedisPool.getResource()){

            //pre sync
            Pipeline pipe = jedis.pipelined();
            LocalDateTime timestamp = LocalDateTime.now();

            pipe.zadd(getKey(name),
                    dateToInt(timestamp),
                    String.format("%s:%s", name, UUID.randomUUID().toString()));

            //remove all values from set that has has existed for more than time to live duration
            pipe.zremrangeByScore(getKey(name),
                     0,
                    dateToInt(timestamp.minusSeconds(seconds)));

            // get amount of items in list
            Response<Long> rCount = pipe.zcard(getKey(name));
            pipe.sync();

            if(rCount.get() > maxHits){
                throw new RateLimitExceededException();
            }
        }
    }

    private String getKey(final String name){
        return String.format("%s:%s", name, id.toString());
    }

    private long dateToInt(LocalDateTime date){
        return date.getHour() * 60 +
                date.getMinute() * 60 +
                date.getSecond();
    }
}
