package com.linus.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
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
public class DPCLogStreaming {

    public static void main (String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf ().setMaster ("spark://192.168.1.100:7077").setAppName ("DPCLogStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext (conf, Durations.seconds (10));
        Map<String, Object> kafkaParams = new HashMap<> ();
//        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("bootstrap.servers", "192.168.1.110:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "dpc_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("dpc");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, Integer> wordCounts = stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, Integer> () {
                    @Override
                    public Tuple2<String, Integer> call(ConsumerRecord<String, String> record) {
                        String value = record.value ();
                        String[] tokens = value.split ("\u0001");
                        if (tokens[0].trim ().equals ("dpc_js")) {
                            return new Tuple2<> (tokens[5], 1);
                        } else if (tokens[0].trim ().equals ("dpc_visit")) {
                            return new Tuple2<> (tokens[1], 1);
                        } else {
                            return new Tuple2<> ("", 1);
                        }
                    }
                }).reduceByKey (new Function2<Integer, Integer, Integer> () {
            @Override
            public Integer call (Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print ();
        jssc.start ();
        jssc.awaitTermination ();
    }
}
