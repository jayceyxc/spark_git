package com.linus.dataalgorithms.spark.mba;

import com.linus.dataalgorithms.utils.Combination;
import com.linus.dataalgorithms.utils.Tuple3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FindAssociationRules {

    static JavaSparkContext createJavaSparkContext() throws Exception {
        SparkConf conf = new SparkConf ();
        conf.setAppName ("market-basket-analysis");
        // 建立一个快读串行化器
        conf.set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // 现在默认为32MB缓冲区而不是64MB
        conf.set("spark.kryoserializer.buffer.mb", "32");
        JavaSparkContext context = new JavaSparkContext (conf);

        return context;
    }

    static List<String> toList(String transaction) {
        String[] items = transaction.trim ().split (",");
        List<String> list = new ArrayList<String> ();

        for (String item : items) {
            list.add (item);
        }

        return list;
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || list.isEmpty ()) {
            return list;
        }

        if ((i < 0) || (i > list.size () - 1)) {
            return list;
        }

        List<String> cloned = new ArrayList<String> (list);
        cloned.remove (i);

        return cloned;
    }

    public static void main (String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println ("Usage: FindAssociationRules <transactions>");
            System.exit (1);
        }

        String transactionsFileName = args[0];

        // 创建一个Spark上下文对象
        JavaSparkContext context = createJavaSparkContext ();

        // 从HDFS读取所有交易并创建第一个RDD
        JavaRDD<String> transactions = context.textFile (transactionsFileName, 1);
        transactions.saveAsTextFile ("rules/output/1");

        // 生成频繁模式
        // PairFlatMapFunction<T, K, V>
        // T => Iterable<Tuple2<K, V>>
        JavaPairRDD<List<String>, Integer> patterns = transactions.flatMapToPair (new PairFlatMapFunction<String, List<String>, Integer> () {
            @Override
            public Iterable<Tuple2<List<String>, Integer>> call (String transaction) throws Exception {
                List<String> list = toList (transaction);
                List<List<String>> combinations = Combination.findSortedCombinations (list);
                List<Tuple2<List<String>, Integer>> result = new ArrayList<Tuple2<List<String>, Integer>> ();
                for (List<String> combList : combinations) {
                    if (combList.size () > 0) {
                        result.add (new Tuple2<List<String>, Integer> (combList, 1));
                    }
                }

                return result;
            }
        });

        // 组合/规约频繁模式
        JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey (new Function2<Integer, Integer, Integer> () {
            @Override
            public Integer call (Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 现在可以得到: patterns(K, V)
        // K = 模式(List<String>)
        // V = 模式频度
        // 现在给定 (K, V) 为(List<a, b, c>, 2),将生成以下(K2, V2)对
        // (List<a, b, c>, T2(null, 2))
        // (List<a, b>, T2(List<a, b, c>, 2))
        // (List<a, c>, T2(List<a, b, c>, 2))
        // (List<b, c>, T2(List<a, b, c>, 2))

        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subpatterns =
                combined.flatMapToPair (new PairFlatMapFunction<
                        Tuple2<List<String>, Integer>,  // T
                        List<String>,                   // K
                        Tuple2<List<String>, Integer>   // V
                        > () {
                    @Override
                    public Iterable<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call (Tuple2<List<String>, Integer> pattern) throws Exception {
                        List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result =
                                new ArrayList<Tuple2<List<String>, Tuple2<List<String>, Integer>>> ();
                        List<String> list = pattern._1;
                        Integer frequency = pattern._2;
                        result.add (new Tuple2<> (list, new Tuple2<> (null, frequency)));
                        if (list.size () == 1) {
                            return result;
                        }

                        // 模式中包含多个商品
                        // result.add(new Tuple2(list, new Tuple2(null, size))
                        for (int i = 0; i < list.size (); i++) {
                            List<String> subList = removeOneItem (list, i);
                            result.add (new Tuple2<> (subList, new Tuple2<> (list, frequency)));
                        }

                        return result;
                    }
                });

        // 组合子模式
        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subpatterns.groupByKey ();

        // 生成关联规则
        // Now use (K=List<String>, V=Iterable<Tuple2<List<String>, Integer>>)
        // 生成关联规则
        // JavaRDD<R> map(Function<T, R>, f)
        // 通过对这个RDD的所有元素应用一个函数返回一个新的RDD
        JavaRDD<List<Tuple3<List<String >, List<String >, Double>>> assocRules = rules.map (new Function<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, List<Tuple3<List<String>, List<String>, Double>>> () {
            @Override
            public List<Tuple3<List<String>, List<String>, Double>> call (Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> in) throws Exception {
                List<Tuple3<List<String >, List<String >, Double>> result = new ArrayList<Tuple3<List<String >, List<String >, Double>>();
                List<String> fromList = in._1;
                Iterable<Tuple2<List<String >, Integer>> to = in._2;
                List<Tuple2<List<String>, Integer>> toList = new ArrayList<Tuple2<List<String>, Integer>> ();
                Tuple2<List<String>, Integer> fromCount = null;
                for (Tuple2<List<String>, Integer> t2 : to) {
                    // 找到"count"对象
                    if (t2._1 == null) {
                        fromCount = t2;
                    } else {
                        toList.add (t2);
                    }
                }

                // 现在我们得到了生成关联规则所需的对象
                // "fromList", "fromCount" 和 "toList"
                if (toList.isEmpty ()) {
                    // 没有生成输出，不过由于Spark不接受null对象，我们将模拟一个null对象
                    return result; // 一个空列表
                }

                // 现在使用3个对象"from","fromCount"和"toList" 创建关联规则
                for (Tuple2<List<String>, Integer> t2 : toList) {
                    double confidence = (double) t2._2 / (double) fromCount._2;
                    List<String> t2List = new ArrayList<String> (t2._1);
                    t2List.removeAll (fromList);
                    result.add (new Tuple3<> (fromList, t2List, confidence));
                }

                return result;

            }
        });

        context.close ();

        System.exit (0);
    }
}
