package com.linus.spark_streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Test the operstions on mongodb in spark streaming.
 *
 * https://docs.mongodb.com/spark-connector/v2.1/java/write-to-mongodb/
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 11 15:00
 */
public class SparkStreamingMongoOp {
    public static void main (String[] args) {
        String checkPointDir = "file:///tmp/mongo_check_point";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate (checkPointDir, new Function0<JavaStreamingContext> () {
            @Override
            public JavaStreamingContext call () throws Exception {
                SparkConf conf = new SparkConf ()
                        .setAppName ("SparkStreamingMongoOp")
                        .set ("spark.mongodb.input.uri", "mongodb://115.29.165.122:19191/bidder_conf.config")
                        .set ("spark.mongodb.output.uri", "mongodb://115.29.165.122:19191/bidder_conf.config")
                        .set ("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner");
                return new JavaStreamingContext (conf, Durations.seconds (1));
            }
        });
        jssc.checkpoint (checkPointDir);

        // Load data and infer schema, disregard toDF() name as it returns Dataset
        Dataset<Row> implicitDS = MongoSpark.load (jssc.sparkContext ()).toDF ();
        implicitDS.printSchema ();
        implicitDS.show ();
        implicitDS.createOrReplaceTempView ("characters");

        // Load data with explicit schema
//        Dataset<Character> explicitDS = MongoSpark.load (jssc.sparkContext ()).toDS (Character.class);
//        explicitDS.printSchema ();
//        explicitDS.show ();

        // Create the temp view and execute the query
//        explicitDS.createOrReplaceTempView ("characters");
        SparkSession session = SparkSession.builder ().config (jssc.sparkContext ().getConf ()).getOrCreate ();
        Dataset<Row> result = session.sql ("SELECT command, keyword, value FROM characters WHERE value >= 30");

        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String> ();
        readOverrides.put ("database", "bidder_spark");
        readOverrides.put ("collection", "test");

        // Write the data to the "test" collection
        MongoSpark.write (result).options (readOverrides).mode ("overwrite").save ();

        // Load the data from the "test" collection
        ReadConfig readConfig = ReadConfig.create (jssc.sparkContext ()).withOptions (readOverrides);
        MongoSpark.load(jssc.sparkContext (), readConfig).toDF ().show ();
    }
}
