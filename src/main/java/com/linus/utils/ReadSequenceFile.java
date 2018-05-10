package com.linus.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 04 15:25
 */
public class ReadSequenceFile {
    public static void main (String[] args) {
        try {
            String uri = args[0];
            Configuration conf = new Configuration ();
            FileSystem fs = FileSystem.get (URI.create (uri), conf);
            Path path = new Path (uri);

            SequenceFile.Reader reader = null;
            try {
                //返回 SequenceFile.Reader 对象
                reader = new SequenceFile.Reader (conf, SequenceFile.Reader.file (path.makeQualified(URI.create (uri), fs.getWorkingDirectory ())));
                // 获得Sequence中使用的类型
                Writable key = (Writable) ReflectionUtils.newInstance (reader.getKeyClass (), conf);
                Writable value = (Writable) ReflectionUtils.newInstance (reader.getValueClass (), conf);

                long position = reader.getPosition ();
                //next（）方法迭代读取记录 直到读完返回false
                while (reader.next (key, value)) {
                    String syncSeen = reader.syncSeen () ? "*" : "";
                    System.out.printf ("[%s%s]\t%s\t%s\n", position, syncSeen, key, value.toString ());
                    // 读取下一条记录
                    position = reader.getPosition ();
                }
            } finally {
                IOUtils.closeStream (reader);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace ();
        }
    }
}
