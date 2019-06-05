package com.linus.examples;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.linus.utils.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class JsonFormatExample {

    static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
        @Override
        public Iterator<Person> call (Iterator<String> stringIterator) throws Exception {
            ArrayList<Person> people = new ArrayList<> ();
            ObjectMapper mapper = new ObjectMapper ();
            while (stringIterator.hasNext ()) {
                String line = stringIterator.next ();
                System.out.println ("original content: " + line);
                try {
                    people.add (mapper.readValue (line, Person.class));
                } catch( IOException ioe) {
                    ioe.printStackTrace ();
                }
            }

            return people.iterator ();
        }
    }

    static class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
        @Override
        public Iterator<String> call (Iterator<Person> people) throws Exception {
            ArrayList<String> text = new ArrayList<> ();
            ObjectMapper mapper = new ObjectMapper ();
            while (people.hasNext ()) {
                Person person = people.next ();
                text.add (mapper.writeValueAsString (person));
            }

            return text.iterator ();
        }
    }

    public static void main (String[] args) {
        System.out.println (args.length);
        System.out.println (Arrays.toString (args));
        if (args.length < 2) {
            System.err.println ("Usage: JsonFormatExample <input path> <out path>");
            System.exit (1);
        }

        SparkConf conf = new SparkConf ().setMaster ("spark://192.168.1.110:7077").setAppName ("json example");
        JavaSparkContext context = new JavaSparkContext (conf);

        String inputPath = args[0];
        String outputPath = args[1];

        Path outputDir = new Path(outputPath);
        Configuration hadoopConf = context.hadoopConfiguration ();
        try {
            FileSystem fileSystem = FileSystem.get (new URI ("hdfs://192.168.1.110:8020"), hadoopConf);
            if (fileSystem.exists (outputDir)) {
                System.out.println ("output path is already exist");
                boolean result = fileSystem.delete (outputDir, true);
                if (result) {
                    System.out.println ("delete the output path successfully");
                }
            }
        } catch (URISyntaxException urise) {
            urise.printStackTrace ();
        } catch (IOException ioe) {
            ioe.printStackTrace ();
        }

        JavaRDD<String> input = context.textFile (inputPath);
        List<String> content = input.collect ();
        for (String line : content) {
            System.out.println (line);
        }
        JavaRDD<Person> result = input.mapPartitions (new ParseJson ());
        List<Person> personList = result.collect ();
        for (Person person : personList) {
            System.out.println (person);
        }
//        result.saveAsTextFile (outputPath);
        JavaRDD<String> stringResult = result.mapPartitions (new WriteJson ());
        stringResult.saveAsTextFile (outputPath);
    }
}
