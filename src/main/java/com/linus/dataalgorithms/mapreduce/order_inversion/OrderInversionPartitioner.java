package com.linus.dataalgorithms.mapreduce.order_inversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition (PairOfWords key, IntWritable intWritable, int numPartitions) {
        // key = (leftWord, rightWord)
        String leftWord = key.getLeftElement ();
        return (int) Math.abs (hash (leftWord) % numPartitions);
    }

    // 由String.hashCode()改写
    private static long hash(String str) {
        long h = 1125899906842597L; // 质数
        int length = str.length ();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt (i);
        }

        return h;
    }
}
