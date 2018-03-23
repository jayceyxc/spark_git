package com.linus.yinni;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import com.linus.utils.MyMultipleOutput;
import com.linus.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class UserTag {
    private static final int ADSL_INDEX = 0;
    private static final int IP_INDEX = 1;
    private static final int URL_INDEX = 2;
    private static final int REFER_INDEX = 3;

    private static final BigDecimal PRESERVE_RATE = BigDecimal.valueOf (0.7);
    private static final BigDecimal MIN_WEIGHT = BigDecimal.valueOf (0.005);
    private static final int MAX_TAG_NUMBER = 10;

    private final static int MIN_TOKENS_LENGTH = 7;

    private static final AhoCorasickDoubleArrayTrie<List<String>> ac = Utils.buildACMachine (Utils.URL_TAGS_FILE_NAME);

    private static Map<String, BigDecimal> normalization (Map<String, BigDecimal> tagValueMap) {
        BigDecimal sum = BigDecimal.ZERO;
        Map<String, BigDecimal> results = new HashMap<> ();
        for (BigDecimal weight : tagValueMap.values ()) {
            sum = sum.add (BigDecimal.valueOf (Math.sqrt (weight.doubleValue ())));
        }

        if (sum.compareTo (BigDecimal.ZERO) > 0) {
            //这里将map.entrySet()转换成list
            List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (tagValueMap.entrySet ());

            //然后通过比较器来实现排序
            /*
            list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
                // 按value值降序排序，如果要升序，将o2和o1位置互换
                @Override
                public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                    return o2.getValue ().compareTo (o1.getValue ());
                }
            });
            */

            // 按value值降序排序，如果要升序，将o2和o1位置互换
            list.sort ((o1, o2) -> o2.getValue ().compareTo (o1.getValue ()));

            for (Map.Entry<String, BigDecimal> mapping : list) {
                results.put (mapping.getKey (), BigDecimal.valueOf (Math.sqrt (mapping.getValue ().doubleValue ())).divide (sum, 2, BigDecimal.ROUND_HALF_UP));
            }
        }

        return results;
    }

    private static void mergeUserTagMap (Map<String, BigDecimal> curTagValueMap, Map<String, BigDecimal> prevTagValueMap) {
        for (String tag : prevTagValueMap.keySet ()) {
            if (curTagValueMap.containsKey (tag)) {
                curTagValueMap.put (tag, curTagValueMap.get (tag).add (prevTagValueMap.get (tag).multiply (PRESERVE_RATE)));
            } else {
                curTagValueMap.put (tag, prevTagValueMap.get (tag).multiply (PRESERVE_RATE));
            }
        }
    }

    public static void main (String[] args) throws URISyntaxException, IOException {
        if (args.length < 1) {
            System.err.println ("Usage: UserTag <visit log> <out_path>");
            System.exit (1);
        }
        SparkConf sparkConf = new SparkConf ().setMaster ("spark://192.168.1.110:7077").setAppName ("user tag");
        JavaSparkContext context = new JavaSparkContext (sparkConf);

        String inputPath = args[0];
        String outputPath = args[1];

        Path outputDir = new Path (outputPath);
        Configuration hadoopConf = context.hadoopConfiguration ();
        FileSystem fileSystem = FileSystem.get (new URI ("hdfs://192.168.1.110:8020"), hadoopConf);
        if (fileSystem.exists (outputDir)) {
            System.out.println ("output path is already exist");
            boolean result = fileSystem.delete (outputDir, true);
            if (result) {
                System.out.println ("delete the output path successfully");
            }
        }

        JavaRDD<String> lines = context.textFile (inputPath, 1);
        JavaPairRDD<String, String> adslTags = lines.flatMapToPair (new PairFlatMapFunction<String, String, String> () {
            @Override
            public Iterable<Tuple2<String, String>> call (String s) throws Exception {
                List<Tuple2<String, String>> results = new ArrayList<> ();
                String[] tokens = s.trim ().split ("\u0001");
                if (tokens.length < MIN_TOKENS_LENGTH) {
                    tokens = s.split ("\t");
                    if (tokens.length == 2) {
                        String adsl = tokens[0];
                        String tagsWeightValue = tokens[1];
                        results.add (new Tuple2<> (adsl, tagsWeightValue));

                        return results;
                    }
                }
                String adsl = tokens[ADSL_INDEX].trim ();
                String ip = tokens[IP_INDEX].trim ();
                String url = Utils.urlFormat (tokens[URL_INDEX].trim ());
                String refer = Utils.urlFormat (tokens[REFER_INDEX].trim ());

                if (adsl.isEmpty ()) {
                    adsl = ip;
                }

                Set<String> finalTagSet = new TreeSet<String> ();
                List<AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>>> tagList = ac.parseText (url);
                if (!tagList.isEmpty ()) {
                    for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                        finalTagSet.addAll (hit.value);
                    }
                }

                tagList.clear ();
                tagList = ac.parseText (refer);
                if (!tagList.isEmpty ()) {
                    for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                        finalTagSet.addAll (hit.value);
                    }
                }

                tagList.clear ();
                String host = Utils.urlToHost (url);
                tagList = ac.parseText (host);
                if (!tagList.isEmpty ()) {
                    for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                        finalTagSet.addAll (hit.value);
                    }
                }

                tagList.clear ();
                String referHost = Utils.urlToHost (refer);
                tagList = ac.parseText (referHost);
                if (!tagList.isEmpty ()) {
                    for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                        finalTagSet.addAll (hit.value);
                    }
                }

                for (String tag : finalTagSet) {
                    results.add (new Tuple2<> (adsl, tag));
                }

                return results;
            }
        });

        Function<String, Map<String, BigDecimal>> createTagsCount = new Function<String, Map<String, BigDecimal>> () {
            @Override
            public Map<String, BigDecimal> call (String tag) throws Exception {
                Map<String, BigDecimal> results = new HashMap<> ();
                results.put (tag, BigDecimal.ONE);

                return results;
            }
        };

        Function2<Map<String, BigDecimal>, String, Map<String, BigDecimal>> addTagsCount = new Function2<Map<String, BigDecimal>, String, Map<String, BigDecimal>> () {
            @Override
            public Map<String, BigDecimal> call (Map<String, BigDecimal> tagsCountMap, String tag) throws Exception {
                BigDecimal oldCount = tagsCountMap.get (tag);
                if (oldCount == null) {
                    tagsCountMap.put (tag, BigDecimal.ONE);
                } else {
                    tagsCountMap.put (tag, oldCount.add (BigDecimal.ONE));
                }

                return tagsCountMap;
            }
        };

        Function2<Map<String, BigDecimal>, Map<String, BigDecimal>, Map<String, BigDecimal>> combine = new Function2<Map<String, BigDecimal>, Map<String, BigDecimal>, Map<String, BigDecimal>> () {
            @Override
            public Map<String, BigDecimal> call (Map<String, BigDecimal> tagsCountMap1, Map<String, BigDecimal> tagsCountMap2) throws Exception {
                for (String tag : tagsCountMap2.keySet ()) {
                    BigDecimal oldCount1 = tagsCountMap1.get (tag);
                    BigDecimal oldCount2 = tagsCountMap2.get (tag);
                    if (oldCount1 == null) {
                        tagsCountMap1.put (tag, oldCount2);
                    } else {
                        tagsCountMap1.put (tag, oldCount1.add (oldCount2));
                    }
                }

                return tagsCountMap1;
            }
        };

        JavaPairRDD<String, Map<String, BigDecimal>> adslTagsCount = adslTags.combineByKey (createTagsCount, addTagsCount, combine);

        JavaPairRDD<String, Map<String, BigDecimal>> adslTagsWeight = adslTagsCount.mapValues (new Function<Map<String, BigDecimal>, Map<String, BigDecimal>> () {
            @Override
            public Map<String, BigDecimal> call (Map<String, BigDecimal> tagsCountMap) throws Exception {
                Map<String, BigDecimal> results = new HashMap<> ();
                Map<String, BigDecimal> prevTagsWeightMap = new HashMap<> ();
                String previousTagKey = "";
                for (String key : tagsCountMap.keySet ()) {
                    if (key.contains (":")) {
                        String[] tagValueArray = key.split (",");
                        for (String tagValue : tagValueArray) {
                            String[] tagValuePair = tagValue.split (":");
                            String tag = tagValuePair[0];
                            BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                            prevTagsWeightMap.put (tag, tagWeight);
                        }
                        previousTagKey = key;
                    }
                }

                tagsCountMap.remove (previousTagKey);

                if (tagsCountMap.isEmpty ()) {
                    results.putAll (prevTagsWeightMap);
                } else {
                    results = normalization (tagsCountMap);
                    if (!prevTagsWeightMap.isEmpty ()) {
                        mergeUserTagMap (results, prevTagsWeightMap);
                        results = normalization (results);
                    }
                }

                return results;
            }
        });

        JavaPairRDD<String, Map<String, BigDecimal>> sortedAdslTagsCount = adslTagsWeight.sortByKey ();
        JavaPairRDD<String, String> sortedAdslTagResult = sortedAdslTagsCount.mapToPair (new PairFunction<Tuple2<String, Map<String, BigDecimal>>, String, String> () {
            @Override
            public Tuple2<String, String> call (Tuple2<String, Map<String, BigDecimal>> adslTags) throws Exception {
                String adsl = adslTags._1;
                Map<String, BigDecimal> tagsWeight = adslTags._2;
                int count = 0;
                //这里将map.entrySet()转换成list
                Set<Map.Entry<String, BigDecimal>> entries = tagsWeight.entrySet ();
                if (entries.isEmpty ()) {
                    System.err.println ("entry set is empty");
                }
                List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (entries);
                //然后通过比较器来实现排序
                /*
                list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
                    // 按value值降序排序，如果要升序，将o2和o1位置互换
                    // 如果value值一样，按key值升序排序
                    @Override
                    public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                        if (o1.getValue ().equals (o2.getValue ())) {
                            return o1.getKey ().compareTo (o2.getKey ());
                        } else {
                            return o2.getValue ().compareTo (o1.getValue ());
                        }
                    }
                });
                */

                // 按value值降序排序，如果要升序，将o2和o1位置互换,如果value值一样，按key值升序排序
                list.sort ((o1, o2) -> o1.getValue ().equals (o2.getValue ()) ? o1.getKey ().compareTo (o2.getKey ()) : o2.getValue ().compareTo (o1.getValue ()));

                StringBuilder sb = new StringBuilder ();
                for (Map.Entry<String, BigDecimal> mapping : list) {
                    if (mapping.getValue ().compareTo (MIN_WEIGHT) < 0 || count >= MAX_TAG_NUMBER) {
                        break;
                    }

                    if (sb.length () == 0) {
                        sb.append (String.format ("%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
                    } else {
                        sb.append (String.format (",%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
                    }
                    count += 1;
                }

                return new Tuple2<> (adsl, sb.toString ());
            }
        });

        sortedAdslTagResult.saveAsHadoopFile (outputPath, NullWritable.class, String.class, MyMultipleOutput.class);
    }
}
