package com.linus.spark_sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// col("...") is preferable to df.col("...")
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * The example of spark sql
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 April 29 06:28
 */
public class JavaSparkSQLExample {

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName () {
            return name;
        }

        public void setName (String name) {
            this.name = name;
        }

        public int getAge () {
            return age;
        }

        public void setAge (int age) {
            this.age = age;
        }
    }

    private static void runBasicDataFrameExample(SparkSession session) throws AnalysisException {
        Dataset<Row> df = session.read ().json ("data/people.json");
        // Displays the content of the DataFrame to stdout
        df.show ();
        /**
         +----+-------+
         | age|   name|
         +----+-------+
         |null|Michael|
         |  30|   Andy|
         |  19| Justin|
         +----+-------+
         */

        // Print the schema in a tree format
        df.printSchema ();
        /**
         root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)
         */

        // Select only the "name" column
        df.select (col("name")).show ();
        /**
         +-------+
         |   name|
         +-------+
         |Michael|
         |   Andy|
         | Justin|
         +-------+
         */

        // Select everybody, but increment the age by 1
        df.select (col ("name"), col ("age").plus (1)).show ();
        /**
         +-------+---------+
         |   name|(age + 1)|
         +-------+---------+
         |Michael|     null|
         |   Andy|       31|
         | Justin|       20|
         +-------+---------+
         */

        // Select people older than 21
        df.filter (col ("age").gt (21)).show ();
        /**
         +---+----+
         |age|name|
         +---+----+
         | 30|Andy|
         +---+----+
         */

        // Count people by age
        df.groupBy (col ("age")).count ().show ();
        /**
         +----+-----+
         | age|count|
         +----+-----+
         |  19|    1|
         |null|    1|
         |  30|    1|
         +----+-----+
         */

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView ("people");

        Dataset<Row> sqlDF = session.sql ("SELECT * FROM people");
        sqlDF.show ();
        /**
         +----+-------+
         | age|   name|
         +----+-------+
         |null|Michael|
         |  30|   Andy|
         |  19| Justin|
         +----+-------+
         */

        /**
         * Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
         * If you want to have a temporary view that is shared among all sessions and keep alive until the Spark
         * application terminates, you can create a global temporary view. Global temporary view is tied to a system
         * preserved database global_temp, and we must use the qualified name to refer it, e.g.
         * SELECT * FROM global_temp.view1.
         */

        // Register the DataFrame as a global temporary view
        df.createGlobalTempView ("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        session.sql ("SELECT * FROM global_temp.people").show ();
        /**
         +----+-------+
         | age|   name|
         +----+-------+
         |null|Michael|
         |  30|   Andy|
         |  19| Justin|
         +----+-------+
         */

        // Global temporary view is cross-session
        session.newSession ().sql ("SELECT * FROM global_temp.people").show ();
        /**
         +----+-------+
         | age|   name|
         +----+-------+
         |null|Michael|
         |  30|   Andy|
         |  19| Justin|
         +----+-------+
         */
    }

    private static void runDatasetCreationExample(SparkSession session) {
        // Create an instance of a Bean class
        Person person = new Person ();
        person.setName ("Andy");
        person.setAge (32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean (Person.class);
        Dataset<Person> javaBeanDS = session.createDataset (Collections.singletonList (person), personEncoder);
        javaBeanDS.show ();
        /**
         +---+----+
         |age|name|
         +---+----+
         | 32|Andy|
         +---+----+
         */


        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT ();
        Dataset<Integer> primitiveDS = session.createDataset (Arrays.asList (1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map (new MapFunction<Integer, Integer> () {
            @Override
            public Integer call (Integer value) throws Exception {
                return value + 1;
            }
        }, integerEncoder);
        Integer[] result = (Integer[]) transformedDS.collect ();
        System.out.println (result);
        /**
         * [Ljava.lang.Integer;@61d61b0e
         */

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        Dataset<Person> peopleDS = session.read ().json ("data/people.json").as (personEncoder);
        peopleDS.show ();
        /**
         +----+-------+
         | age|   name|
         +----+-------+
         |null|Michael|
         |  30|   Andy|
         |  19| Justin|
         +----+-------+
         */
    }

    private static void inferSchemaUsingReflection(SparkSession session) {
        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = session.read ()
                .textFile ("data/people.txt")
                .javaRDD ()
                .map (line -> {
                    String[] parts = line.split (",");
                    Person person = new Person ();
                    person.setName (parts[0]);
                    person.setAge (Integer.parseInt (parts[1].trim ()));
                    return person;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = session.createDataFrame (peopleRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView ("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = session.sql ("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING ();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map (
                (MapFunction<Row, String>) row -> "Name: " + row.getString (0),
                stringEncoder);
        teenagerNamesByIndexDF.show ();
        /**
         +------------+
         |       value|
         +------------+
         |Name: Justin|
         +------------+
         */

        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map (
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show ();
        /**
         +------------+
         |       value|
         +------------+
         |Name: Justin|
         +------------+
         */
    }

    private static void programSpecifySchema(SparkSession session) {
        // Create an RDD
        JavaRDD<String> peopleRDD = session.sparkContext ()
                .textFile ("data/people.txt", 1)
                .toJavaRDD ();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<> ();
        for (String fieldName : schemaString.split (" ")) {
            StructField field = DataTypes.createStructField (fieldName, DataTypes.StringType, true);
            fields.add (field);
        }

        StructType schema = DataTypes.createStructType (fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map ((Function<String, Row>) record -> {
            String[] attributes = record.split (",");
            return RowFactory.create (attributes[0], attributes[1].trim ());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = session.createDataFrame (rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView ("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = session.sql ("SELECT name from people");

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map (
                (MapFunction<Row, String>) row -> "Name: " + row.getString (0), Encoders.STRING ());
        namesDS.show ();
        /**
         +-------------+
         |        value|
         +-------------+
         |Name: Michael|
         |   Name: Andy|
         | Name: Justin|
         +-------------+
         */
    }

    /**
     * Spark SQL supports two different methods for converting existing RDDs into Datasets. The first method uses
     * reflection to infer the schema of an RDD that contains specific types of objects. This reflection based approach
     * leads to more concise code and works well when you already know the schema while writing your Spark application.
     * The second method for creating Datasets is through a programmatic interface that allows you to construct a schema
     * and then apply it to an existing RDD. While this method is more verbose, it allows you to construct Datasets when
     * the columns and their types are not known until runtime.
     *
     * Spark SQL supports automatically converting an RDD of JavaBeans into a DataFrame. The BeanInfo, obtained using
     * reflection, defines the schema of the table. Currently, Spark SQL does not support JavaBeans that contain Map
     * field(s). Nested JavaBeans and List or Array fields are supported though. You can create a JavaBean by creating
     * a class that implements Serializable and has getters and setters for all of its fields.
     *
     * @param session
     */
    private static void runInteroperatingRDDExample(SparkSession session) {
        inferSchemaUsingReflection (session);
        programSpecifySchema (session);
    }

    public static void main (String[] args) throws AnalysisException{
        SparkSession session = SparkSession
                .builder ()
                .appName ("JavaSparkSQLExample")
                .config ("spark.some.config.option", "some-value")
                .getOrCreate ();


        runBasicDataFrameExample (session);
        runDatasetCreationExample (session);
        runInteroperatingRDDExample (session);
    }
}
