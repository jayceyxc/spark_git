package com.linus.spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile (" ");

    public static void main (String[] args) {
        SparkConf conf = new SparkConf ().setMaster ("spark://192.168.1.110:7077").setAppName ("JavaNetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext (conf, Durations.seconds (10));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream ("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);

        //flatMap is a DStream operation that creates a new DStream by generating multiple new records from each record in the source DStream.
        JavaDStream<String> words = lines.flatMap (
                new FlatMapFunction<String, String> () {
                    @Override
                    public Iterable<String> call (String s) throws Exception {
                        return Arrays.asList (s.split (" "));
                    }
                }
        );

        // Count each in each batch
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair (
                new PairFunction<String, String, Integer> () {
                    @Override
                    public Tuple2<String, Integer> call (String s) throws Exception {
                        return new Tuple2<String, Integer> (s, 1);
                    }
                }
        ).reduceByKey (
                new Function2<Integer, Integer, Integer> () {
                    @Override
                    public Integer call (Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

//        JavaDStream<String> words = lines.flatMap (x -> Arrays.asList (SPACE.split (x)).iterator ());
//        JavaPairDStream<String, Integer> wordCounts = words.mapToPair (s -> new Tuple2<> (s, 1)).reduceByKey ((i1, i2) -> i1 + i2);

        wordCounts.print ();
        jssc.start ();
        jssc.awaitTermination ();
    }
}
