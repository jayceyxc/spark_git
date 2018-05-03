package com.linus.dataalgorithms.spark.leftouterjoin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class LeftOuterJoin {
    public static void main (String[] args) {
        // 读取输入参数
        if (args.length < 2) {
            System.err.println ("Usage: LeftOuterJoin <users> <transactions>");
            System.exit(1);
        }

        String usersInputFile = args[0];
        String transcationsInputFile = args[1];
        System.out.println ("users=" + usersInputFile);
        System.out.println ("transactions=" + transcationsInputFile);

        // 创建一个JavaSparkContext对象
        SparkConf conf = new SparkConf ().setAppName ("leftOuterJoin").setMaster ("spark://192.168.1.110:7077");
        JavaSparkContext ctx = new JavaSparkContext (conf);

        JavaRDD<String> users = ctx.textFile (usersInputFile);
        // <K2, V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> f)
        // 通过对这个RDD的所有元素应用一个函数返回一个新的RDD
        // PairFunction<T, K, V> 其中 T => Tuple2<K, V>
        JavaPairRDD<String, Tuple2<String, String>> usersRDD = users.mapToPair (new PairFunction<String, String, Tuple2<String, String>> () {
            @Override
            public Tuple2<String, Tuple2<String, String>> call (String s) throws Exception {
                String[] userRecord = s.split ("\t");
                Tuple2<String, String> location = new Tuple2<String, String> ("L", userRecord[1]);
                return new Tuple2<String, Tuple2<String, String>> (userRecord[0], location);
            }
        });

        // 为交易传建一个JavaRDD
        JavaRDD<String> transcations = ctx.textFile (transcationsInputFile);

        // <K2, V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> f)
        // 通过对这个RDD的所有元素应用一个函数返回一个新的RDD
        // PairFunction<T, K, V> 其中 T => Tuple2<K, V>
        JavaPairRDD<String, Tuple2<String, String>> transactionsRDD = transcations.mapToPair (new PairFunction<String, String, Tuple2<String, String>> () {
            @Override
            public Tuple2<String, Tuple2<String, String>> call (String s) throws Exception {
                String[] transcationRecord = s.split ("\t");
                Tuple2<String, String> product = new Tuple2<String, String> ("P", transcationRecord[1]);
                return new Tuple2<String, Tuple2<String, String>> (transcationRecord[2], product);
            }
        });

        // 创建RDD并集
        JavaPairRDD<String, Tuple2<String, String>> allRDD = transactionsRDD.union (usersRDD);
        allRDD.saveAsTextFile ("leftouterjoin.txt");

        // 调用groupByKey创建JavaPairRDD(userID, List<T2>), 按userID对allRDD分组
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey ();

        JavaPairRDD<String, String> productLocationRDD = groupedRDD.flatMapToPair (new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, String> () {
            @Override
            public Iterator<Tuple2<String, String>> call (Tuple2<String, Iterable<Tuple2<String, String>>> s) throws Exception {
                String userID = s._1;
                Iterable<Tuple2<String, String>> pairs = s._2;
                String location = "UNKNOWN";
                List<String> products = new ArrayList<String> ();
                for (Tuple2<String, String> t2 : pairs) {
                    if (t2._1.equals ("L")) {
                        location = t2._2;
                    } else {
                        // t2._1.equals("P")
                        products.add (t2._2);
                    }
                }

                // 现在发出(K, V)对
                List<Tuple2<String, String>> kvList = new ArrayList<Tuple2<String, String>> ();
                for (String product : products) {
                    kvList.add (new Tuple2<String, String> (product, location));
                }

                // 需要说明，边必须是双向的，也就是说，每个{source, destination} 边必须有一个相应的{destination， source}

                return kvList.iterator ();
            }
        });

        // 查找一个商品的所有地址，结果将是JavaPairRDD<String, List<String>>
        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationRDD.groupByKey ();

        List<Tuple2<String, Iterable<String>>> debug3 = productByLocations.collect ();
        System.out.println ("-- debug3 begin---");
        for (Tuple2<String, Iterable<String>> t2 : debug3) {
            System.out.println ("debug3: t2._1=" + t2._1);
            System.out.println ("debug3: t2._2=" + t2._2);
        }
        System.out.println ("-- debug3 end---");

        // 对输出做最终处理，将值从List<String> 改为Tuple2<Set<String>, Integer>,其中包含唯一地址及相应数目的一个集合
        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = productByLocations.mapValues (new Function<Iterable<String>, Tuple2<Set<String>, Integer>> () {
            @Override
            public Tuple2<Set<String>, Integer> call (Iterable<String> v1) throws Exception {
                Set<String> uniqueLocations = new HashSet<String> ();
                for (String location : v1) {
                    uniqueLocations.add (location);
                }

                return new Tuple2<Set<String>, Integer> (uniqueLocations, uniqueLocations.size ());
            }
        });

        // 打印最终结果RDD
        System.out.println ("=== Unique Locations and Counts ===");
        List<Tuple2<String, Tuple2<Set<String>, Integer>>> debug4 = productByUniqueLocations.collect ();
        System.out.println ("=== debug4 begin ====");
        for (Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
            System.out.println ("debug4 t2._1=" + t2._1);
            System.out.println ("debug4 t2._2=" + t2._2);
        }
        System.out.println ("=== debug4 end ====");
    }
}
