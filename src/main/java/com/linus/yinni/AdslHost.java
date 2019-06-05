package com.linus.yinni;

import com.linus.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;

public class AdslHost {
    private static final int ADSL_INDEX = 0;
    private static final int IP_INDEX = 1;
    private static final int URL_INDEX = 2;

    private static class Comp implements Comparator<Tuple2<String, String>>, Serializable {
        @Override
        public int compare (Tuple2<String, String> o1, Tuple2<String, String> o2) {
            int adsl_result = o1._1.compareTo (o2._1);
            if (adsl_result == 0) {
                return o1._2.compareTo (o2._2);
            } else {
                return adsl_result;
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

        Path outputDir = new Path(outputPath);
        Configuration hadoopConf = context.hadoopConfiguration ();
        FileSystem fileSystem = FileSystem.get(new URI ("hdfs://192.168.1.110:8020"), hadoopConf);
        if (fileSystem.exists (outputDir)) {
            System.out.println ("output path is already exist");
            boolean result = fileSystem.delete (outputDir, true);
            if (result) {
                System.out.println ("delete the output path successfully");
            }
        }

        JavaRDD<String> lines = context.textFile (inputPath, 1);
        JavaPairRDD<Tuple2<String, String>, Integer> adslHosts = lines.mapToPair (new PairFunction<String, Tuple2<String, String>, Integer> () {

            @Override
            public Tuple2<Tuple2<String, String>, Integer> call (String s) throws Exception {
                String[] tokens = s.trim ().split ("\u0001");
                String adsl = tokens[ADSL_INDEX].trim ();
                String ip = tokens[IP_INDEX].trim ();
                String url = tokens[URL_INDEX].trim ();
                if (adsl.isEmpty ()) {
                    adsl = ip;
                }

                String host = Utils.urlToHost (url);

                Tuple2<String, String> key = new Tuple2<> (adsl, host);
                return new Tuple2<> (key, 1);
            }
        });

        JavaPairRDD<Tuple2<String, String>, Integer> adslHostCount = adslHosts.reduceByKey (new Function2<Integer, Integer, Integer> () {
            @Override
            public Integer call (Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Tuple2<String, String>, Integer> sortedAdslHostCount = adslHostCount.sortByKey (new Comp());
        sortedAdslHostCount.saveAsTextFile (outputPath);
//        Map<Tuple2<String, String>, Integer> adslHostMap = sortedAdslHostCount.collectAsMap ();
//        int count = 0;
//        for (Map.Entry<Tuple2<String, String>, Integer> entry : adslHostMap.entrySet ()) {
//            System.out.printf ("adsl: %s, host: %s, count: %d\n", entry.getKey ()._1, entry.getKey ()._2, entry.getValue ());
//            count++;
//            if (count == 100) {
//                break;
//            }
//        }
    }
}
