#!/usr/bin/env bash

spark-submit --master spark://192.168.1.100:7077 --class com.linus.spark_sql.UntypedAverage target/spark_git.jar
