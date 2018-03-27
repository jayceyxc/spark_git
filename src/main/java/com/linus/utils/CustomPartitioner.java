package com.linus.utils;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner{

    private int numPartitions;

    public CustomPartitioner(int i) {
        numPartitions =i;
    }

    @Override
    public int numPartitions()
    {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key){

        //partition based on the first character of the key...you can have your logic here !!
        return Math.abs (((String)key).hashCode() % numPartitions);

    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof CustomPartitioner)
        {
            CustomPartitioner partitionerObject = (CustomPartitioner)obj;
            if(partitionerObject.numPartitions == this.numPartitions)
                return true;
        }

        return false;
    }
}
