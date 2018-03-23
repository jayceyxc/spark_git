package com.linus.dataalgorithms.mapreduce.leftouterjoin;


import com.linus.dataalgorithms.utils.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class LeftJoinReducer extends Reducer<Pair<String, Integer>, Pair<String, String>, Text, Text> {
    /**
     * @param key     为userid
     * @param values  为List<Pair<left, right></>在这里
     *                values = List< {
     *                Pair<"L", locationID>
     *                Pair<"P", productID1>
     *                Pair<"P", productID2>
     *                ...
     *                }
     *                ></>
     *                需要说明， Pair<"L", locationID>在所有商品对之前到达，第一个值是未知，
     *                如果不是，说明得到的不是一个用户记录，所以要把locationID设置为"undefined"
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce (Pair<String, Integer> key, Iterable<Pair<String, String>> values, Context context) throws IOException, InterruptedException {
        Text locationID = new Text("undefined");
        for (Pair<String, String> value : values) {
            if (value.getLeft ().equals ("L")) {
                locationID.set(value.getRight ());
                continue;
            }

            // 这里是一个商品value.getLeftElement ().equals ("")
            Text productID = new Text(value.getRight ());
            context.write (locationID, productID);
        }
    }
}
