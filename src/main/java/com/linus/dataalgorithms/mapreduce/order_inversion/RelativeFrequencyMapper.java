package com.linus.dataalgorithms.mapreduce.order_inversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelativeFrequencyMapper extends Mapper<Object, Text, PairOfWords, IntWritable> {
    private int neighborWindow = 2;
    private PairOfWords pair = new PairOfWords ();
    private IntWritable one = new IntWritable (1);

    @Override
    protected void setup (Context context) throws IOException, InterruptedException {
        // 驱动器将设置"neighbor.window"
        neighborWindow = context.getConfiguration ().getInt ("neighbor.window", 2);
    }

    /**
     *
     * @param key 由系统生成, 这里忽略
     * @param value 一个String, 单词集
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map (Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString ().split (" ");
        if (tokens.length < 2) {
            return;
        }

        for (int i = 0; i < tokens.length; i++) {
            String word = tokens[i];
            pair.setWord (word);
            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;

            for (int j = start; j <= end; j++) {
                if (i == j) {
                    continue;
                }

                pair.setNeighbor (tokens[j]);
                context.write (pair, one);
            }
            pair.setNeighbor ("*");
            int totalCount = end - start;
            context.write (pair, new IntWritable (totalCount));
        }
    }
}
