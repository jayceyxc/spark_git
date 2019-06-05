package com.linus.dataalgorithms.mapreduce.leftouterjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocationCountMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map (Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write (key, value);
    }
}
