package com.linus.dataalgorithms.mapreduce.mba;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable outputValue = new IntWritable ();

    // key形式为Tuple2(Si, Sj)
    // value = List<Integer>, 其中各个元素分别是一个整数
    @Override
    protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get ();
        }

        outputValue.set (sum);
        context.write (key, outputValue);
    }
}
