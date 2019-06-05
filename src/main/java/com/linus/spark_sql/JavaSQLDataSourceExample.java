package com.linus.spark_sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 04 10:06
 */
public class JavaSQLDataSourceExample {

    public static void main (String[] args) {
        SparkSession session = SparkSession
                .builder ()
                .appName ("JavaSQLDataSourceExample")
                .getOrCreate ();

        Dataset<Row> usersDF = session.read ().load ("data/users.parquet");
        usersDF.show ();
        /**
         +------+--------------+----------------+
         |  name|favorite_color|favorite_numbers|
         +------+--------------+----------------+
         |Alyssa|          null|  [3, 9, 15, 20]|
         |   Ben|           red|              []|
         +------+--------------+----------------+
         */
        usersDF.select ("name", "favorite_color").write ().save ("data/namesAndFavColors.parquet");

        /**
         * You can also manually specify the data source that will be used along with any extra options that you would
         * like to pass to the data source. Data sources are specified by their fully qualified name
         * (i.e., org.apache.spark.sql.parquet), but for built-in sources you can also use their short names
         * (json, parquet, jdbc, orc, libsvm, csv, text).
         */
        Dataset<Row> peopleDF = session.read ().format ("json").load ("data/people.json");
        peopleDF.select ("name", "age").write ().format ("parquet").save ("data/namesAndAges.parquet");

        Dataset<Row> peopleDFCsv = session.read ().format ("csv")
                .option ("sep" , ":")
                .option ("inferSchema", "true")
                .option ("header", "true")
                .load ("data/people.csv");
        peopleDFCsv.show ();

        // Instead of using read API to load a file into DataFrame and query it, you can also query that file directly with SQL.
        Dataset<Row> sqlDF = session.sql ("SELECT * FROM parquet.`data/users.parquet`");
        sqlDF.show ();
    }
}
