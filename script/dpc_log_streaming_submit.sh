#!/usr/bin/env bash

#spark-submit --master spark://192.168.1.100:7077 --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar --class com.linus.spark_streaming.DPCLogStreaming target/spark_git.jar
#spark-submit --master spark://192.168.1.100:7077 --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar --class com.linus.spark_streaming.DPCLogVisitUrl target/spark_git.jar

#spark-submit --master spark://192.168.1.100:7077 --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar --class com.linus.spark_streaming.RtbLogStat target/spark_git.jar

spark-submit --master spark://192.168.1.100:7077 --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar,lib/mongo-spark-connector_2.10-2.1.0.jar,lib/mongo-java-driver-3.4.2.jar --class com.linus.spark_streaming.RtbLogStatUpdateKeyVersion target/spark_git.jar

spark-submit --master spark://192.168.1.100:7077 --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar,lib/mongo-spark-connector_2.10-2.1.0.jar,lib/mongo-java-driver-3.4.2.jar --class com.linus.spark_streaming.SparkStreamingMongoOp target/spark_git.jar


spark-submit --master spark://192.168.1.100:7077 --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar,lib/jedis-2.9.0.jar,lib/commons-pool2-2.4.2.jar --class com.linus.spark_streaming.SparkStreamingRedisOp target/spark_git.jar 115.29.173.59:9092 bidder redis://:yxc@127.0.0.1:63791/4

spark-submit --master spark://192.168.1.100:7077 --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar,lib/jedis-2.9.0.jar,lib/commons-pool2-2.4.2.jar --class com.linus.spark_streaming.RtbLogStatRedisVersion target/spark_git.jar 115.29.173.59:9092 bidder redis://:yxc@127.0.0.1:63791/4