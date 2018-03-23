package com.linus.dataalgorithms.mapreduce.order_inversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {
    @Override
    protected void reduce (PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int partialSum = 0;
        for (IntWritable value : values) {
            partialSum += value.get ();
        }

        context.write (key, new IntWritable (partialSum));
    }
}
