package com.linus.dataalgorithms.mapreduce.mba;

import com.linus.dataalgorithms.utils.Combination;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;

    // 输出key: 成对项列表，可以是2或3 ...
    private static final Text reduceKey = new Text();

    // 输出value: 项列表中的成对项数
    private static final IntWritable NUMBER_ONE = new IntWritable (1);

    // 由setup()函数读取，由驱动器设置
    int numberOfPairs;

    @Override
    protected void setup (Context context) throws IOException, InterruptedException {
        this.numberOfPairs = context.getConfiguration ().getInt ("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
    }

    @Override
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString ().trim ();
        List<String> items = convertItemsToList (line);
        if ((items == null) || (items.isEmpty ())) {
            return;
        }
        generateMapperOutput (numberOfPairs, items, context);
    }

    private static List<String> convertItemsToList(String line) {
        if ((line == null) || line.length () == 0) {
            return null;
        }

        String[] tokens = StringUtils.split (line, ",");
        if ((tokens == null) || (tokens.length == 0)) {
            return null;
        }

        List<String> items = new ArrayList<String> ();
        for (String token : tokens) {
            if (token != null) {
                items.add (token.trim ());
            }
        }

        return items;
    }

    /**
     * 这个方法通过对输入列表排序建立一个键值对集合
     * 键是交易中的一个商品组合，值=1
     * 这里需要排序以确保(a,b)和(b,a)表示相同的键
     * @param numberOfPairs 关联商品对数
     * @param items 商品列表，来自输入行
     * @param context Hadoop任务上下文
     */
    private void generateMapperOutput(int numberOfPairs, List<String> items, Context context) throws IOException, InterruptedException{
        List<List<String>> sortedCombinations = Combination.findSortedCombinations (items, numberOfPairs);
        for (List<String> itemList : sortedCombinations) {
            System.out.println("itemlist="+itemList.toString());
            reduceKey.set(itemList.toString ());
            context.write (reduceKey, NUMBER_ONE);
        }
    }
}
