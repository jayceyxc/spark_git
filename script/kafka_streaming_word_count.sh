#!/usr/bin/env bash

spark-submit --jars lib/spark-streaming-kafka-0-10_2.10-2.1.0.jar,lib/kafka-clients-0.10.0.1.jar,lib/kafka_2.10-0.10.0.1.jar,lib/scala-library-2.10.6.jar --class com.linus.spark_streaming.JavaDirectKafkaWordCount target/spark_git.jar 192.168.1.110:9092 dpc