package com.linus.dataalgorithms.mapreduce.leftouterjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 定义reduce()完成地址统计
 */
public class LocationCountReducer extends Reducer<Text, Text, Text, Integer> {
    @Override
    protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<Text> set = new HashSet<Text> ();
        for (Text value : values) {
            set.add (value);
        }

        context.write (key, set.size ());
    }
}
