package com.linus.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * statistic the bidder log, get the show and click count of a ad.
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 10 15:33
 */
public class RtbLogStatMapWithState {

    static class ExtractUrl implements PairFunction<ConsumerRecord<String, String>, String, Integer> {
        @Override
        public Tuple2<String, Integer> call (ConsumerRecord<String, String> record) throws Exception {
            String value = record.value ();
            String[] tokens = value.split ("\u0001");
            System.out.println ("key: " + record.key () + ", value: " + record.value ());
            if (tokens[0].trim ().equals ("rtb_creative")) {
                return new Tuple2<> (tokens[23], 1);
            } else {
                return new Tuple2<> ("", 1);
            }
        }
    }

    public static void main (String[] args) throws InterruptedException {
        String checkPointDir = "file:///tmp/bidder_check_point";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate (checkPointDir, new Function0<JavaStreamingContext> () {
            @Override
            public JavaStreamingContext call () throws Exception {
                SparkConf conf = new SparkConf ().setAppName ("RtbLogStatMapWithState");
                return new JavaStreamingContext (conf, Durations.seconds (5));
            }
        });

        File checkpointDir = new File (checkPointDir);
        checkpointDir.mkdir();
        checkpointDir.deleteOnExit();
//        String streamingCheckPointDir = "hdfs://192.168.1.110:8020/user/yuxuecheng/bidder_streaming_check_point";
        jssc.checkpoint (checkPointDir);
        Map<String, Object> kafkaParams = new HashMap<> ();
        kafkaParams.put ("bootstrap.servers", "115.29.173.59:9092");
        kafkaParams.put ("key.deserializer", StringDeserializer.class);
        kafkaParams.put ("value.deserializer", StringDeserializer.class);
        kafkaParams.put ("group.id", "bidder_group");
        kafkaParams.put ("auto.offset.reset", "latest");
//        kafkaParams.put ("auto.offset.reset", "earliest");
        kafkaParams.put ("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList ("bidder");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream (
                        jssc,
                        LocationStrategies.PreferConsistent (),
                        ConsumerStrategies.<String, String>Subscribe (topics, kafkaParams)
                );

        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunction = (url, one, state) -> {
            int sum = one.orElse (0) + (state.exists () ? state.get () : 0);
            Tuple2<String, Integer> output = new Tuple2<> (url, sum);
            state.update (sum);

            return output;
        };

        JavaPairDStream<String, Integer> wordCounts = stream.mapToPair (
                new ExtractUrl ())
                .filter ((Function<Tuple2<String, Integer>, Boolean>) v1 -> {
                    if (!v1._1.isEmpty ()) {
                        return true;
                    } else {
                        return false;
                    }
                });
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> streamedUrlCounts = wordCounts.mapWithState (StateSpec.function (mappingFunction));

//        JavaDStream<Tuple2<String, Integer>> filterStream = streamedUrlCounts.filter ((Function<Tuple2<String, Integer>, Boolean>) v1 -> {
//            if (v1._2 > 0) {
//                return true;
//            } else {
//                return false;
//            }
//        });

        streamedUrlCounts.foreachRDD ((VoidFunction2<JavaRDD<Tuple2<String, Integer>>, Time>) (v1, v2) -> {
            System.out.println ("-------------------------------------------");
            System.out.println ("Time: " + v2);
            System.out.println ("-------------------------------------------");
            v1.foreachPartition ((VoidFunction<Iterator<Tuple2<String, Integer>>>) tuple2Iterator -> {
                while (tuple2Iterator.hasNext ()) {
                    Tuple2<String, Integer> value = tuple2Iterator.next ();
                    System.out.println ("url: " + value._1 + ", count: " + value._2);
                }
            });
        });

//        streamedUrlCounts.print ();
        jssc.start ();
        jssc.awaitTermination ();
    }
}
