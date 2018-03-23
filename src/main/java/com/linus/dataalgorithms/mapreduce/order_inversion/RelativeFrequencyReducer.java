package com.linus.dataalgorithms.mapreduce.order_inversion;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {
    private double totalCount = 0;
    private String currentWord = "NOT_DEFINED";
    private DoubleWritable outputValue = new DoubleWritable ();

    @Override
    protected void reduce (PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.getRightElement ().equals ("*")) {
            if (key.getLeftElement ().equals (currentWord)) {
                totalCount += getTotalCount (values);
            } else {
                currentWord = key.getLeftElement ();
                totalCount = getTotalCount (values);
            }
        } else {
            int count = getTotalCount (values);
            double relativeCount = count / totalCount;
            outputValue.set (relativeCount);
            context.write (key, outputValue);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get ();
        }

        return count;
    }
}
