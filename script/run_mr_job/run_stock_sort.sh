#!/bin/sh

OUT_PATH=data_algorithms/output/stock_data
IN_PATH=data_algorithms/input/stock_data.txt

hadoop fs -rm -r -skipTrash ${OUT_PATH}

echo ${IN_PATH} ${OUT_PATH}

hadoop jar spark_git.jar com.linus.dataalgorithms.mapreduce.secondary_sort_v2.SecondarySortDriver ${IN_PATH} ${OUT_PATH}

echo "Totally done!!"
exit $?
