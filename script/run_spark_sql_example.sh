#!/bin/sh

spark-submit --master spark://192.168.1.3:7077 --class com.linus.spark_sql.JavaSparkSQLExample target/spark_git.jar
