#!/bin/sh

spark-submit --master spark://192.168.1.110:7077 --class com.linus.dataalgorithms.spark.secondary_sort.SecondarySortUsingGroupByKey spark_git.jar hdfs://192.168.1.110:8020/user/yuxuecheng/data_algorithms/input/ch01/sample_input.txt hdfs://192.168.1.110:8020/user/yuxuecheng/data_algorithms/output/ch01/sample_output.txt
