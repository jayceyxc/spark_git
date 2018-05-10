#!/usr/bin/env bash

#spark-submit --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar --class com.linus.spark_streaming.DPCLogStreaming target/spark_git.jar
spark-submit --driver-java-options "-Dlog4j.configuration=file:///Users/yuxuecheng/Learn/Source/github/spark_git/conf/log4j.properties" --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar --class com.linus.spark_streaming.DPCLogVisitUrl target/spark_git.jar