#!/bin/sh

USERS=hdfs://192.168.1.110:8020/user/yuxuecheng/data_algorithms/input/leftouterjion/users.txt
TRANSACTIONS=hdfs://192.168.1.110:8020/user/yuxuecheng/data_algorithms/input/leftouterjion/transactions.txt
spark-submit --master spark://192.168.1.110:7077 --class com.linus.dataalgorithms.spark.leftouterjoin.LeftOuterJoin spark_git.jar ${USERS} ${TRANSACTIONS}

