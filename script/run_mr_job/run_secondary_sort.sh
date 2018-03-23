#!/bin/sh

OUT_PATH=secondary_sort/output
IN_PATH=secondary_sort/input/*

hadoop fs -rm -r -skipTrash ${OUT_PATH}

echo ${IN_PATH} ${OUT_PATH}

hadoop jar spark_git.jar com.linus.dataalgorithms.mapreduce.secondary_sort.SecondarySortDriver ${IN_PATH} ${OUT_PATH}

echo "Totally done!!"
exit $?
