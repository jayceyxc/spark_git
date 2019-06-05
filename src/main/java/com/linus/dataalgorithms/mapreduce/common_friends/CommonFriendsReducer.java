package com.linus.dataalgorithms.mapreduce.common_friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputValue = new Text();

    public static Set<String> intersection(Set<String> user1friends,
                                            Set<String> user2friends) {
        if ((user1friends == null) || user1friends.isEmpty ()) {
            return null;
        }

        if ((user2friends == null) || user2friends.isEmpty ()) {
            return null;
        }

        // 两个集合都非null
        if (user1friends.size () < user2friends.size ()) {
            return intersect (user1friends, user2friends);
        } else {
            return intersect (user2friends, user1friends);
        }
    }

    private static Set<String> intersect(Set<String> smallSet,
                                          Set<String> largeSet) {
        Set<String> result = new TreeSet<String> ();
        // 迭代处理小集合来提高性能
        for (String x : smallSet) {
            if (largeSet.contains (x)) {
                result.add (x);
            }
        }

        return result;
    }

    private Set<String> stringToSet(String value) {
        String[] tokens = value.split (",");
        return new TreeSet<> (Arrays.asList (tokens));
    }

    @Override
    protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> result = new TreeSet<> ();
        for (Text value : values) {
            Set<String> friends = stringToSet (value.toString ());
            if (result.isEmpty ()) {
                result.addAll (friends);
            } else {
                result = intersection (result, friends);
            }
        }
        String commonFriends = StringUtils.join (result, ",");
        outputValue.set (commonFriends);
        context.write (key, outputValue);
    }
}
