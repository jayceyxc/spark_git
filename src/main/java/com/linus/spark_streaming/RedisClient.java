package com.linus.spark_streaming;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis client that implements KryoSerializable
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 14 14:39
 */
public class RedisClient implements KryoSerializable {

    public static JedisPool jedisPool;
    public String host;

    public RedisClient() {
        Runtime.getRuntime ().addShutdownHook (new CleanWorkThread());
    }

    public RedisClient(String host) {
        this.host = host;
        Runtime.getRuntime ().addShutdownHook (new CleanWorkThread ());
        jedisPool = new JedisPool (new GenericObjectPoolConfig (), host);
    }

    public Jedis getResource() {
        return jedisPool.getResource ();
    }

    @Override
    public void write (Kryo kryo, Output output) {
        kryo.writeObject (output, host);
    }

    @Override
    public void read (Kryo kryo, Input input) {
        host = kryo.readObject (input, String.class);
        this.jedisPool = new JedisPool (host);

    }

    static class CleanWorkThread extends Thread {
        /**
         * If this thread was constructed using a separate
         * <code>Runnable</code> run object, then that
         * <code>Runnable</code> object's <code>run</code> method is called;
         * otherwise, this method does nothing and returns.
         * <p>
         * Subclasses of <code>Thread</code> should override this method.
         *
         * @see #start()
         * @see #stop()
         */
        @Override
        public void run () {
            System.out.printf ("Destroy jedis pool");
            if (null != jedisPool) {
                jedisPool.close ();
                jedisPool = null;
            }
        }
    }
}
