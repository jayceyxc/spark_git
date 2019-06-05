package com.linus.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 09 21:44
 *
 *
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ spark-submit com.linus.spark_streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 *
 */
public class JavaDirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile (" ");

    public static void main (String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

//        Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf ().setAppName ("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext (sparkConf, Durations.seconds (2));

        Set<String> topicsSet = new HashSet<> (Arrays.asList (topics.split (",")));
        Map<String, Object> kafkaParams = new HashMap<> ();
//        kafkaParams.put ("metadata.broker.list", brokers);
        kafkaParams.put ("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "dpc_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

//        Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream (
                jssc,
                LocationStrategies.PreferConsistent (),
                ConsumerStrategies.Subscribe (topicsSet, kafkaParams)
        );

//        Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map (ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap (x -> Arrays.asList (SPACE.split (x)).iterator ());
        JavaPairDStream<String, Integer> wordsCounts = words.mapToPair (s -> new Tuple2<> (s, 1))
                .reduceByKey ((i1, i2) -> i1 + i2);
        wordsCounts.print ();

//        Start the computation
        jssc.start ();
        jssc.awaitTermination ();
    }
}
