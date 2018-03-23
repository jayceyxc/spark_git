package com.linus.utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

public class MyMultipleOutput extends MultipleOutputFormat<String, String>{
    private TextOutputFormat<String, String> output = null;

    @Override
    protected String generateFileNameForKeyValue(String key, String value,
                                                 String name) {
//        String[] split = key.split(",");
//        String device=split[0];
//        String year=split[1].substring(0,4);
//        String month=split[1].substring(5,7);
//        String day=split[1].substring(8,10);
        /*name是reduce任务默认文件名，注意如果这里返回的文件名不追加name，
         *就会造成多个reduce获取到的文件名都是day，多个reduce写一个文件，文件内容只会有一个reduce输出的内容
         */
        return key.substring (0, 2);

    }

    @Override
    protected RecordWriter<String, String> getBaseRecordWriter(FileSystem fs,
                                                               JobConf job, String name, Progressable arg3) throws IOException {
        if (output == null) {
            output = new TextOutputFormat<String, String>();
        }
        return output.getRecordWriter(fs, job, name, arg3);
    }

}

