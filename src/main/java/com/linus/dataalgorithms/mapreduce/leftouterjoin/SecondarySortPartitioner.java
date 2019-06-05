package com.linus.dataalgorithms.mapreduce.leftouterjoin;

import com.linus.dataalgorithms.utils.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<Pair<Text, Text>, Pair<Text, Text>> {
    @Override
    public int getPartition (Pair<Text, Text> key, Pair<Text, Text> value, int numPartitions) {
        return (key.getLeft ().hashCode () & Integer.MAX_VALUE) % numPartitions;
    }
}
