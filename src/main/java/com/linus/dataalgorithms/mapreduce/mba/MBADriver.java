package com.linus.dataalgorithms.mapreduce.mba;

import com.linus.dataalgorithms.utils.HadoopUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MBADriver extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger (MBADriver.class);

    public static void main (String[] args) throws Exception {
        if (args.length != 3) {
            printUsage ();
            System.exit (1);
        }

        int exitStatus = ToolRunner.run (new MBADriver (), args);
        logger.info ("exit status=" + exitStatus);
        System.exit (exitStatus);
    }

    private static void printUsage() {
        System.out.println("USAGE: [input-path] [output-path] [number-of-pairs]");
        ToolRunner.printGenericCommandUsage (System.out);
    }

    @Override
    public int run (String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfPairs = Integer.parseInt (args[2]);

        logger.info("input path: " + inputPath);
        logger.info ("output path: " + outputPath);
        logger.info ("number of pairs: " + numberOfPairs);

        // Job configuration
        Job job = Job.getInstance (getConf ());
        job.setJobName ("MBADriver");
        job.getConfiguration ().setInt ("number.of.pairs", numberOfPairs);

        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache (job, "/lib/");

        // input/output path
        FileInputFormat.setInputPaths (job, new Path (inputPath));
        FileOutputFormat.setOutputPath (job, new Path (outputPath));

        // Mapper K, V output
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (IntWritable.class);
        // output format
        job.setOutputFormatClass (TextOutputFormat.class);

        // Reduce K, V output
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (IntWritable.class);

        // set mapper/reducer
        job.setMapperClass (MBAMapper.class);
        job.setCombinerClass (MBAReducer.class);
        job.setReducerClass (MBAReducer.class);

        // delete the output path if exists to avoid "existing dir/file" error
        Path outputDir = new Path (outputPath);
        FileSystem.get(getConf ()).delete (outputDir, true);

        long startTime = System.currentTimeMillis ();
        boolean status = job.waitForCompletion (true);
        logger.info ("job status=" + status);
        long endTime = System.currentTimeMillis ();
        long elapsedTime = endTime - startTime;
        logger.info ("Elapsed time: " + elapsedTime + " milliseconds");

        return status ? 0 : 1;
    }
}
