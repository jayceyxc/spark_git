#!/bin/sh

OUT_PATH=data_algorithms/out/topn/unique
IN_PATH=data_algorithms/input/topn/unique/topn.txt

hadoop fs -rm -r -skipTrash ${OUT_PATH}

echo ${IN_PATH} ${OUT_PATH}

hadoop jar spark_git.jar com.linus.dataalgorithms.mapreduce.topn.TopNDriver 10 ${IN_PATH} ${OUT_PATH}

echo "Totally done!!"
exit $?
