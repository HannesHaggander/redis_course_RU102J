package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.HostPort;
import com.redislabs.university.RU102J.TestKeyManager;
import org.junit.*;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RateLimiterSlidingDaoRedisImplTest {

    private static JedisPool jedisPool;
    private static Jedis jedis;
    private static TestKeyManager keyManager;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        jedisPool = new JedisPool(HostPort.getRedisHost(), HostPort.getRedisPort());
        jedis = new Jedis(HostPort.getRedisHost(), HostPort.getRedisPort());
        keyManager = new TestKeyManager("test");
    }

    @AfterClass
    public static void tearDown() {
        jedisPool.destroy();
        jedis.close();
    }

    @After
    public void flush() {
        keyManager.deleteKeys(jedis);
    }

    @Test
    public void hit() {
        int exceptionCount = 0;
        RateLimiter limiter = new RateLimiterSlidingDaoImpl(jedisPool, 60, 10);
        for (int i=0; i<10; i++) {
            try {
                limiter.hit("foo");
            } catch (RateLimitExceededException e) {
                exceptionCount += 1;
            }
        }

        assertThat(exceptionCount, is(0));
    }

    @Test
    public void hitException(){
        int exceptionCount = 0;
        RateLimiter limiter = new RateLimiterSlidingDaoImpl(jedisPool, 60, 5);
        for (int i=0; i<10; i++) {
            try {
                limiter.hit("foo");
            } catch (RateLimitExceededException e) {
                exceptionCount += 1;
            }
        }

        assertThat(exceptionCount, is(5));
    }

    @Test
    public void hitWaitSendInappropriate(){
        int exceptionCount = 0;
        int seconds = 2;

        RateLimiter limiter = new RateLimiterSlidingDaoImpl(jedisPool, seconds, 5);

        for (int i=0; i<5; i++) {
            try {
                limiter.hit("foo");
            } catch (RateLimitExceededException e) {
                exceptionCount += 1;
            }
        }

        try {
            Thread.sleep((seconds-1)*1000);
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }

        for (int i=0; i<5; i++) {
            try {
                limiter.hit("foo");
            } catch (RateLimitExceededException e) {
                exceptionCount += 1;
            }
        }

        assertThat(exceptionCount, is(5));
    }


    @Test
    public void hitWaitTest(){
        int exceptionCount = 0;
        int seconds = 2;

        RateLimiter limiter = new RateLimiterSlidingDaoImpl(jedisPool, seconds, 5);

        for (int i=0; i<5; i++) {
            try {
                limiter.hit("foo");
            } catch (RateLimitExceededException e) {
                exceptionCount += 1;
            }
        }

        try {
            Thread.sleep((seconds+1)*1000);
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }

        for (int i=0; i<5; i++) {
            try {
                limiter.hit("foo");
            } catch (RateLimitExceededException e) {
                exceptionCount += 1;
            }
        }

        assertThat(exceptionCount, is(0));
    }

//    @Test
//    public void getMinuteOfDayBlock() {
//    }
}
