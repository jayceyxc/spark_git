package com.linus.dataalgorithms.mapreduce.common_friends;

import com.linus.dataalgorithms.utils.HadoopUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CommonFriendsDriver extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger (CommonFriendsDriver.class);
    @Override
    public int run (String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance (getConf ());
        job.setJobName ("Common Friends");
        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache (job, "/lib/");

        // 设置输入输出
        FileInputFormat.setInputPaths (job, new Path (inputPath));
        FileOutputFormat.setOutputPath (job, new Path (outputPath));

        // 设置mapper输出的key value 类型
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (Text.class);

        job.setOutputFormatClass (TextOutputFormat.class);

        // 设置reducer输出的key value 类型
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (Text.class);

        // 设置map reduce类
        job.setJarByClass (CommonFriendsDriver.class);
        job.setMapperClass (CommonFriendsMapper.class);
        job.setReducerClass (CommonFriendsReducer.class);

        Path outputDir = new Path(outputPath);
        FileSystem.get (getConf ()).delete (outputDir, true);

        long startTime = System.currentTimeMillis ();
        boolean status = job.waitForCompletion (true);
        logger.info ("job status: " + status);
        long endTime = System.currentTimeMillis ();
        logger.info ("use time: " + (endTime - startTime) + " milliseconds");

        return status ? 0 : 1;
    }

    public static void main (String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println ("Usage [input-path] [output-path]");
            ToolRunner.printGenericCommandUsage (System.out);
            System.exit (1);
        }

        int exitStatus = ToolRunner.run (new CommonFriendsDriver (), args);
        logger.info ("exit status=" + exitStatus);
        System.exit (exitStatus);
    }
}
