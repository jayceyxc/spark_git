package com.linus.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

/**
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 14 15:08
 */
public class SparkStreamingRedisOp {

    static class ExtractUrl implements PairFunction<ConsumerRecord<String, String>, String, Integer> {
        @Override
        public Tuple2<String, Integer> call (ConsumerRecord<String, String> record) throws Exception {
            String line = record.value ();
            String[] tokens = line.split ("\u0001");
            System.out.println ("key: " + record.key () + ", value: " + record.value ());
            if (tokens[0].trim ().equals ("rtb_creative")) {
                return new Tuple2<> (tokens[23], 1);
            } else {
                return new Tuple2<> ("", 1);
            }
        }
    }

    static class AddUrlCount implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call (Integer v1, Integer v2) throws Exception {
            return v1 + v2;
        }
    }

    static class SubtractUrlCount implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call (Integer v1, Integer v2) throws Exception {
            return v1 - v2;
        }
    }

    public static void main (String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.err.println ("Usage: kafka_spark_redis <brokers> <topics> <redisServer>\n" +
                    "  <brokers> Kafka broker列表\n" +
                    "  <topics> 要消费的topic列表\n" +
                    " <redisServer> redis 服务器地址 \n\n");
            System.exit (1);
        }

        /* 解析参数 */
        String brokers = args[0];
        String topics = args[1];
        String redisServer = args[2];

        String checkPointDir = "file:///tmp/redis_check_point";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate (checkPointDir, new Function0<JavaStreamingContext> () {
            @Override
            public JavaStreamingContext call () throws Exception {
                SparkConf conf = new SparkConf ()
                        .setAppName ("SparkStreamingRedisOp")
                        .set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                return new JavaStreamingContext (conf, Durations.seconds (1));
            }
        });
        jssc.checkpoint (checkPointDir);
        JavaSparkContext jsc = jssc.sparkContext ();

        HashSet<String> topicSet = new HashSet<> (Arrays.asList (topics.split (",")));
        HashMap<String, Object> kafkaParams = new HashMap<> ();
        kafkaParams.put ("bootstrap.servers", brokers);
        kafkaParams.put ("key.deserializer", StringDeserializer.class);
        kafkaParams.put ("value.deserializer", StringDeserializer.class);
        kafkaParams.put ("group.id", "redis_test");
        kafkaParams.put ("auto.offset.reset", "latest");
//        kafkaParams.put ("auto.offset.reset", "earliest");
        kafkaParams.put ("enable.auto.commit", false);

        //创建redis连接池管理类
        RedisClient redisClient = new RedisClient (redisServer);

        // 创建Redis连接池管理对象
        final Broadcast<RedisClient> broadcastRedis = jsc.broadcast (redisClient);

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream (
                        jssc,
                        LocationStrategies.PreferConsistent (),
                        ConsumerStrategies.<String, String>Subscribe (topicSet, kafkaParams)
                );

        JavaPairDStream<String, Integer> urlCounts = stream.mapToPair (
                new ExtractUrl ()
        ).filter (new Function<Tuple2<String, Integer>, Boolean> () {
            @Override
            public Boolean call (Tuple2<String, Integer> v1) throws Exception {
                if (v1._1.isEmpty ()) {
                    return false;
                } else {
                    return true;
                }
            }
        }).reduceByKeyAndWindow (
                new AddUrlCount (),
                new SubtractUrlCount (),
                Durations.seconds (30),
                Durations.seconds (10)
        ).filter (new Function<Tuple2<String, Integer>, Boolean> () {
            @Override
            public Boolean call (Tuple2<String, Integer> v1) throws Exception {
                if (v1._2 > 0) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        urlCounts.foreachRDD (new VoidFunction2<JavaPairRDD<String, Integer>, Time> () {
            @Override
            public void call (JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
                System.out.println ("-------------------------------------------");
                System.out.println ("Time: " + v2);
                System.out.println ("-------------------------------------------");
                v1.foreachPartition (new VoidFunction<Iterator<Tuple2<String, Integer>>> () {
                    @Override
                    public void call (Iterator<Tuple2<String, Integer>> valueIterator) throws Exception {
                        String url;
                        Integer count;
                        RedisClient redisClient = broadcastRedis.getValue ();
                        Jedis jedis = redisClient.getResource ();
                        while (valueIterator.hasNext ()) {
                            Tuple2<String, Integer> value = valueIterator.next ();
                            url = value._1;
                            count = value._2;
                            jedis.incrBy (url, count);
                        }
                        jedis.close ();
                    }
                });
            }
        });

        jssc.start ();
        jssc.awaitTermination ();
    }
}
