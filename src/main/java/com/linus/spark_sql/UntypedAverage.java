package com.linus.spark_sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Untyped User-Defined Aggregate Functions
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 02 09:42
 */
public class UntypedAverage {
    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage () {
            List<StructField> inputFields = new ArrayList<> ();
            inputFields.add (DataTypes)
        }

        @Override
        public StructType inputSchema () {
            return null;
        }

        @Override
        public StructType bufferSchema () {
            return null;
        }

        @Override
        public DataType dataType () {
            return null;
        }

        @Override
        public boolean deterministic () {
            return false;
        }

        @Override
        public void initialize (MutableAggregationBuffer buffer) {

        }

        @Override
        public void update (MutableAggregationBuffer buffer, Row input) {

        }

        @Override
        public void merge (MutableAggregationBuffer buffer1, Row buffer2) {

        }

        @Override
        public Object evaluate (Row buffer) {
            return null;
        }
    }
}
