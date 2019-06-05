package com.linus.dataalgorithms.spark.common_friends;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class FindCommonFriends {
    public static void main (String[] args) {
//      检查输入参数
        if (args.length < 1) {
            System.err.println ("Usage: FindCommonFriends <file>");
            System.exit (1);
        }
        System.out.println ("HDFS input file: " + args[0]);

//      创建一个JavaSparkContext对象
        JavaSparkContext context = new JavaSparkContext ();

//      从HDFS读取输出文本文件并创建第一个从JavaRDD表示输入文件
        JavaRDD<String> records = context.textFile (args[0]);

//      将JavaRDD<String>映射到键值对,其中key=Tuple2<user1, user2>, value = 好友列表
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> pairs = records.flatMapToPair (new PairFlatMapFunction<String, Tuple2<Long, Long>, Iterable<Long>> () {
            @Override
            public Iterator<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> call (String s) throws Exception {
                String[] tokens = s.split (",");
                long person = Long.parseLong (tokens[0]);
                String friendsAsString = tokens[1];
                String[] friendsTokenized = friendsAsString.split (" ");
                if (friendsTokenized.length == 1) {
                    Tuple2<Long, Long> key = buildSortedTuple (person, Long.parseLong (friendsTokenized[0]));
                    return Arrays.asList (new Tuple2<Tuple2<Long, Long>, Iterable<Long>> (key, new ArrayList<Long> ())).iterator ();
                }

                List<Long> friends = new ArrayList<Long> ();
                for (String f : friendsTokenized) {
                    friends.add (Long.parseLong (f));
                }

                List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> result = new ArrayList<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> ();
                for (Long f : friends) {
                    Tuple2<Long, Long> key = buildSortedTuple (person, f);
                    result.add (new Tuple2<Tuple2<Long, Long>, Iterable<Long>> (key, friends));
                }

                return result.iterator ();
            }
        });

        // debug1
        List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> debug1 = pairs.collect ();
        for (Tuple2<Tuple2<Long, Long>, Iterable<Long>> t2 : debug1) {
            System.out.println ("debug1 key=" + t2._1 + "\t value=" + t2._2);
        }
//      将(key=Tuple2<u1, u2>，value=List<friends>)对规约为
//      (key=Tuple2<u1, u2>, value=List<List<friends>>)
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Iterable<Long>>> grouped = pairs.groupByKey ();
        // debug2
        List<Tuple2<Tuple2<Long, Long>, Iterable<Iterable<Long>>>> debug2 = grouped.collect ();
        for (Tuple2<Tuple2<Long, Long>, Iterable<Iterable<Long>>> t2 : debug2) {
            System.out.println ("debug2 key=" + t2._1 + "\t value=" + t2._2);
        }
//      利用所有List<List<Long>>的交集查找共同好友
        // 要查找共同好友，只需要修改值来得到两个用户所有好友的交集，可以使用
        // JavaPairRDD.mapValues()方法来完成(无需改变键)
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> commonFriends = grouped.mapValues (new Function<Iterable<Iterable<Long>>, Iterable<Long>> () {
            @Override
            public Iterable<Long> call (Iterable<Iterable<Long>> s) throws Exception {
                Map<Long, Integer> countCommon = new HashMap<Long, Integer> ();
                int size = 0;
                for (Iterable<Long> iter : s) {
                    size++;
                    List<Long> list = iterableToList (iter);
                    if ((list == null) || list.isEmpty ()) {
                        continue;
                    }

                    for (Long f : list) {
                        Integer count = countCommon.get (f);
                        if (count == null) {
                            countCommon.put (f, 1);
                        } else {
                            countCommon.put (f, ++count);
                        }
                    }
                }

                // 如果countCommon.Entry<f, count> == countCommon.Entry<f, s.size()>
                // 则有一个共同好友
                List<Long> finalCommonFriends = new ArrayList<Long> ();
                for (Map.Entry<Long, Integer> entry : countCommon.entrySet ()) {
                    if (entry.getValue () == size) {
                        finalCommonFriends.add (entry.getKey ());
                    }
                }

                return finalCommonFriends;
            }
        });

        // debug3
        List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> debug3 = commonFriends.collect ();
        for (Tuple2<Tuple2<Long, Long>, Iterable<Long>> t2 : debug3) {
            System.out.println ("debug3 key=" + t2._1 + "\t value=" + t2._2);
        }

        Map<Tuple2<Long, Long>, Iterable<Long>> commonFriendsMap = commonFriends.collectAsMap ();
        for (Map.Entry<Tuple2<Long, Long>, Iterable<Long>> entry : commonFriendsMap.entrySet ()) {
            System.out.println (entry.getKey () + ":" + entry.getValue ());
        }
    }

    // 建立一个有序的Tuple来避免重复
    private static Tuple2<Long, Long> buildSortedTuple(Long a, Long b) {
        if (a < b) {
            return new Tuple2<Long, Long> (a, b);
        } else {
            return new Tuple2<Long, Long> (b, a);
        }
    }

    private static List<Long> iterableToList(Iterable<Long> iterable) {
        List<Long> result = new ArrayList<Long> ();
        for (Long value : iterable) {
            result.add (value);
        }

        return result;
    }
}
