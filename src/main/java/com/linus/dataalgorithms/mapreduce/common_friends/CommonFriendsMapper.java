package com.linus.dataalgorithms.mapreduce.common_friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    /**
     *
     * @param tokens 用户ID列表，第一个表示自己，后面的表示好友
     * @return 逗号分割的好友列表
     */
    private String buildFriendsString(String[] tokens) {
        if (tokens.length == 1) {
            return "";
        } else if (tokens.length == 2) {
            return tokens[1];
        } else {
            String[] friends = Arrays.copyOfRange (tokens, 1, tokens.length);
            return StringUtils.join (friends, ",");
        }
    }

    private String buildSortedKey(String key1, String key2) {
        if (key1.compareTo (key2) < 0) {
            return key1 + "," + key2;
        } else {
            return key2 + "," + key1;
        }
    }

    @Override
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString ().split (" ");

        String allFriends = buildFriendsString (tokens);
        outputValue.set (allFriends);

        String person = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            String friend = tokens[i];
            String sortedKey = buildSortedKey (person, friend);
            outputKey.set (sortedKey);

            context.write (outputKey, outputValue);
        }
    }
}
