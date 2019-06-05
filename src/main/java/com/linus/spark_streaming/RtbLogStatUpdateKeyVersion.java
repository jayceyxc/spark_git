package com.linus.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.function.ToIntFunction;

/**
 * statistic the bidder log, get the show and click count of a ad.
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 10 15:33
 */
public class RtbLogStatUpdateKeyVersion {

    private static final int UNKNOWN_STATE = 0;
    private static final int CREATIVED_STATE = 1;
    private static final int SHOWED_STATE = 2;
    private static final int CLICKED_STATE = 3;

    private static final int CREATIVE_EVENT = 1;
    private static final int SHOW_EVENT = 2;
    private static final int CLICK_EVENT = 3;

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

    static class ExtractPushIDAndState implements PairFunction<ConsumerRecord<String, String>, String, Integer> {
        @Override
        public Tuple2<String, Integer> call (ConsumerRecord<String, String> record) throws Exception {
            String value = record.value ();
            String[] tokens = value.split ("\u0001");
            System.out.println ("key: " + record.key () + ", value: " + record.value ());
            if (tokens[0].trim ().equals ("rtb_creative")) {
                return new Tuple2<> (tokens[7], CREATIVE_EVENT);
            } else if (tokens[0].trim ().equals ("rtb_show")) {
                return new Tuple2<> (tokens[7], SHOW_EVENT);
            } else if (tokens[0].trim ().equals ("rtb_click")) {
                return new Tuple2<> (tokens[7], CLICK_EVENT);
            } else {
                return new Tuple2<> ("", 0);
            }
        }
    }

    static class UpdatePushidState implements Function2<List<Integer>, Optional<Integer>, Optional<Integer>> {
        @Override
        public Optional<Integer> call (List<Integer> events, Optional<Integer> state) throws Exception {
            Integer finalState = state.orElse (UNKNOWN_STATE);
            List<Integer> sortedEvents = new ArrayList<> (events);
            Collections.sort (sortedEvents);
            for (Integer event : sortedEvents) {
                if (event == CREATIVE_EVENT && finalState == UNKNOWN_STATE) {
                    finalState = CREATIVED_STATE;
                } else if (event == SHOW_EVENT && finalState == CREATIVED_STATE) {
                    finalState = SHOWED_STATE;
                } else if (event == CLICK_EVENT && finalState == SHOWED_STATE) {
                    finalState = CLICKED_STATE;
                }
            }

            return Optional.of (finalState);
        }
    }

    public static void main (String[] args) throws InterruptedException {
//        String checkPointDir = "hdfs://192.168.1.110:8020/user/yuxuecheng/bidder_check_point";
        String checkPointDir = "file:///tmp/bidder_check_point";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate (checkPointDir, new Function0<JavaStreamingContext> () {
            @Override
            public JavaStreamingContext call () throws Exception {
                SparkConf conf = new SparkConf ().setAppName ("RtbLogStatUpdateKeyVersion");
                return new JavaStreamingContext (conf, Durations.seconds (1));
            }
        });

//        String streamingCheckPointDir = "hdfs://192.168.1.110:8020/user/yuxuecheng/bidder_streaming_check_point";
        jssc.checkpoint (checkPointDir);
        Map<String, Object> kafkaParams = new HashMap<> ();
        kafkaParams.put ("bootstrap.servers", "115.29.173.59:9092");
        kafkaParams.put ("key.deserializer", StringDeserializer.class);
        kafkaParams.put ("value.deserializer", StringDeserializer.class);
        kafkaParams.put ("group.id", "bidder_group");
//        kafkaParams.put ("auto.offset.reset", "latest");
        kafkaParams.put ("auto.offset.reset", "earliest");
        kafkaParams.put ("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList ("bidder");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream (
                        jssc,
                        LocationStrategies.PreferConsistent (),
                        ConsumerStrategies.<String, String>Subscribe (topics, kafkaParams)
                );

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new UpdatePushidState ();

        JavaPairDStream<String, Integer> wordCounts = stream.mapToPair (
                new ExtractPushIDAndState ())
                .filter (new Function<Tuple2<String, Integer>, Boolean> () {
                    @Override
                    public Boolean call (Tuple2<String, Integer> v1) throws Exception {
                        if (!v1._1.isEmpty ()) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                })
                .window (Durations.seconds (30), Durations.seconds (5))
                .updateStateByKey (updateFunction)
                .filter (new Function<Tuple2<String, Integer>, Boolean> () {
                    @Override
                    public Boolean call (Tuple2<String, Integer> v1) throws Exception {
                        if (v1._2 > 0) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        wordCounts.print ();
        jssc.start ();
        jssc.awaitTermination ();
    }
}
