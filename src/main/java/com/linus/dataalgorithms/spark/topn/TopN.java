package com.linus.dataalgorithms.spark.topn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class TopN {
    public static void main (String[] args) {
        if (args.length < 1) {
            System.err.println ("Usage: TopN <hdfs-file>");
            System.exit (1);
        }
        String inputPath = args[0];
        System.out.println ("inputPath: <hdfs-file>=" + inputPath);

        // 连接spark master
        SparkConf sparkConf = new SparkConf ().setAppName ("TopN").setMaster ("spark://192.168.1.110:7077");
        JavaSparkContext ctx = new JavaSparkContext (sparkConf);

        // 从HDFS读取输入文件并创建第一个RDD
        JavaRDD<String> lines = ctx.textFile (inputPath, 1);

        // 创建一组Tuple2<String, Integer>
        JavaPairRDD<String, Integer> pairs = lines.mapToPair (new PairFunction<String, String, Integer> () {
            @Override
            public Tuple2<String, Integer> call (String s) throws Exception {
                String[] tokens = s.split (",");
                return new Tuple2<String, Integer> (tokens[0], Integer.parseInt (tokens[1]));
            }
        });

        // 为各个输入分区创建一个本地top 10列表
        JavaRDD<SortedMap<Integer, String>> partions = pairs.mapPartitions (new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>> () {
            @Override
            public Iterable<SortedMap<Integer, String>> call (Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                SortedMap<Integer, String> top10 = new TreeMap<Integer, String> ();
                while (tuple2Iterator.hasNext ()) {
                    Tuple2<String, Integer> tuple = tuple2Iterator.next ();
                    // tuple._1: 唯一键，如cat_id
                    // tuple._2: 项的频度（cat_weight）
                    top10.put (tuple._2, tuple._1);
                    if (top10.size () > 10) {
//                        删除频度最小的元素
                        top10.remove (top10.firstKey ());
                    }
                }
                return Collections.singletonList (top10);
            }
        });

        // 收集所有本地top 10列表，创建一个最终的top 10列表
        SortedMap<Integer, String> finalTop10 = new TreeMap<Integer, String> ();
        List<SortedMap<Integer, String>> allTop10 = partions.collect ();
        for (SortedMap<Integer, String> localTop10 : allTop10) {
            // weight = tuple._1
            // catname = tuple._2
            for (Map.Entry<Integer, String > entry : localTop10.entrySet ()) {
//                System.out.println (entry.getKey () + " -- " + entry.getValue ());
                finalTop10.put (entry.getKey (), entry.getValue ());
                // 只保留top 10
                if (finalTop10.size () > 10) {
                    // 删除频度最小的元素
                    finalTop10.remove (finalTop10.firstKey ());
                }
            }
        }
        System.out.println ("=== top-10 list ===");
        for (Map.Entry<Integer, String> entry : finalTop10.entrySet ()) {
            System.out.println (entry.getKey () + "--" + entry.getValue ());
        }
    }
}
