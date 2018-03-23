package com.linus.programmer_guide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main (String[] args) {
        SparkConf conf = new SparkConf ().setAppName ("test").setMaster ("spark://192.168.1.110:7077");
        JavaSparkContext sc = new JavaSparkContext (conf);
        List<Integer> data = Arrays.asList (1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize (data);
        String filePath = "hdfs://192.168.1.110:8020/user/yuxuecheng/example/data.txt";
        JavaRDD<String> lines = sc.textFile (filePath);
        JavaRDD<Integer> lineLengths = lines.map (s -> s.length ());
        List <Integer> lengthList = lineLengths.collect ();
//        for (int length : lengthList) {
//            System.out.println ("line length: " + length);
//        }
        int totalLength = lineLengths.reduce ((a, b) -> a + b);
        System.out.println ("total length is: " + totalLength);
    }
}
