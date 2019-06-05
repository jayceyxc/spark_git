package com.linus.dataalgorithms.mapreduce.leftouterjoin;

import com.linus.dataalgorithms.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, Pair<String, Integer>, Pair<String, String>> {
    @Override
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString ().split ("\t");
        String productID = tokens[1];
        String userID = tokens[2];
        Pair<String, Integer> outputKey = new Pair<String, Integer>(userID, 2);
        Pair<String, String> outputValue = new Pair<String, String> ("P", productID);
        context.write (outputKey, outputValue);
    }
}
