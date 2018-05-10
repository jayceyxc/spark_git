package com.linus.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Use spark streaming to analysis the dpc log
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 09 18:05
 */
public class DPCLogVisitUrl {

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

    static class ExtractUrl implements PairFunction<ConsumerRecord<String, String>, String, Integer> {
        @Override
        public Tuple2<String, Integer> call (ConsumerRecord<String, String> record) throws Exception {
            String value = record.value ();
            String[] tokens = value.split ("\u0001");
            if (tokens[0].trim ().equals ("dpc_visit")) {
                return new Tuple2<> (tokens[5], 1);
            } else {
                return new Tuple2<> ("", 1);
            }
        }
    }

    public static void main (String[] args) throws InterruptedException {
        String checkPointDir = "hdfs://192.168.1.110:8020/user/yuxuecheng/spark_check_point";
//        SparkConf conf = new SparkConf ().setMaster ("spark://192.168.1.100:7077").setAppName ("JavaNetworkWordCount");
//        JavaStreamingContext jssc = new JavaStreamingContext (conf, Durations.seconds (10));
//        jssc.checkpoint (checkPointDir);
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate (checkPointDir, new Function0<JavaStreamingContext> () {
            @Override
            public JavaStreamingContext call () throws Exception {
                SparkConf conf = new SparkConf ().setMaster ("spark://192.168.1.100:7077").setAppName ("DPCLogVisitUrl");
                return new JavaStreamingContext (conf, Durations.seconds (1));
            }
        });


        String streamingCheckPointDir = "hdfs://192.168.1.110:8020/user/yuxuecheng/spark_streaming_check_point";
        jssc.checkpoint (streamingCheckPointDir);
        Map<String, Object> kafkaParams = new HashMap<> ();
//        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put ("bootstrap.servers", "192.168.1.110:9092");
        kafkaParams.put ("key.deserializer", StringDeserializer.class);
        kafkaParams.put ("value.deserializer", StringDeserializer.class);
        kafkaParams.put ("group.id", "dpc_group");
        kafkaParams.put ("auto.offset.reset", "latest");
        kafkaParams.put ("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList ("dpc");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream (
                        jssc,
                        LocationStrategies.PreferConsistent (),
                        ConsumerStrategies.<String, String>Subscribe (topics, kafkaParams)
                );
//        JavaDStream<ConsumerRecord<String, String>> windowedStream = stream.window (Durations.seconds (30), Durations.seconds (20));

        JavaPairDStream<String, Integer> wordCounts = stream.mapToPair (
                new ExtractUrl ())
                .filter (new Function<Tuple2<String, Integer>, Boolean> () {
                    @Override
                    public Boolean call (Tuple2<String, Integer> v1) throws Exception {
                        if (!v1._1.isEmpty ()) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).reduceByKeyAndWindow (
                        new AddUrlCount (),         // 加上新进入窗口的批次中的元素
                        new SubtractUrlCount (),    // 移除离开窗口的老批次中的元素
                        Durations.seconds (30),     // window duration
                        Durations.seconds (5)       // slide duration
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

//        wordCounts.print ();
        wordCounts.foreachRDD (new VoidFunction2<JavaPairRDD<String, Integer>, Time> () {
            @Override
            public void call (JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
                System.out.println ("-------------------------------------------");
                System.out.println ("Time: " + v2);
                System.out.println ("-------------------------------------------");
                Map<String, Integer> result = v1.collectAsMap ();
                for (Map.Entry<String, Integer> entry : result.entrySet ()) {
                    System.out.println (entry.getKey () + ":\t" + entry.getValue ());
                }
            }
        });
        System.out.println ();
        jssc.start ();
        jssc.awaitTermination ();
    }
}
