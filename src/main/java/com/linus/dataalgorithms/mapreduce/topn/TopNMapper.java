package com.linus.dataalgorithms.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *  Mapper's input are read from SequenceFile and records are: (K, V)
 *    where 
 *          K is a Text
 *          V is an Integer
 * 
 * @author Mahmoud Parsian
 *
 */
public class TopNMapper extends
   Mapper<LongWritable, Text, NullWritable, Text> {

   private int N = 10; // default
   private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

      String line = value.toString ();
      String[] tokens = line.split (",");
      top.put(Integer.parseInt (tokens[1]), line);
      // keep only top N
      if (top.size() > N) {
         top.remove(top.firstKey());
      }
   }
   
   @Override
   protected void setup(Context context) throws IOException,
         InterruptedException {
      this.N = context.getConfiguration().getInt("N", 10); // default is top 10
   }
   

   @Override
   protected void cleanup(Context context) throws IOException,
         InterruptedException {
      for (String str : top.values()) {
         context.write(NullWritable.get(), new Text(str));
      }
   }

}
