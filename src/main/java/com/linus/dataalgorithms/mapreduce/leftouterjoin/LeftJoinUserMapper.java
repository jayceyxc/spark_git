package com.linus.dataalgorithms.mapreduce.leftouterjoin;

import com.linus.dataalgorithms.utils.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LeftJoinUserMapper extends Mapper<LongWritable, Text, Pair<String, Integer>, Pair<String, String>> {
    @Override
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString ().split ("\t");
        String userID = tokens[0];
        String locationID = tokens[1];
        Pair<String, Integer> outputKey = new Pair<String, Integer> (userID, 1);
        Pair<String, String> outputValue = new Pair<String, String> ("L", locationID);
        context.write (outputKey, outputValue);
    }
}
