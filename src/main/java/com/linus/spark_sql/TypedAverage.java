package com.linus.spark_sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;
import java.util.Objects;

/**
 * Type-Safe User-Defined Aggregate Functions
 *
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 04 09:34
 */




public class TypedAverage {

    public static class Employee implements Serializable {
        private String name;
        private Long salary;

        public Employee (String name, Long salary) {
            this.name = name;
            this.salary = salary;
        }

        public Employee () {
            this.name = "";
            this.salary = 0L;
        }

        public String getName () {
            return name;
        }

        public void setName (String name) {
            this.name = name;
        }

        public Long getSalary () {
            return salary;
        }

        public void setSalary (Long salary) {
            this.salary = salary;
        }

        @Override
        public boolean equals (Object o) {
            if (this == o) return true;
            if (o == null || getClass () != o.getClass ()) return false;
            Employee employee = (Employee) o;
            return Objects.equals (name, employee.name) &&
                    Objects.equals (salary, employee.salary);
        }

        @Override
        public int hashCode () {

            return Objects.hash (name, salary);
        }

        @Override
        public String toString () {
            return "Employee{" +
                    "name='" + name + '\'' +
                    ", salary=" + salary +
                    '}';
        }
    }

    public static class Average implements Serializable {
        private long sum;
        private long count;

        public Average (long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public Average() {
            this.sum = 0L;
            this.count = 0L;
        }

        public long getSum () {
            return sum;
        }

        public void setSum (long sum) {
            this.sum = sum;
        }

        public long getCount () {
            return count;
        }

        public void setCount (long count) {
            this.count = count;
        }

        @Override
        public boolean equals (Object o) {
            if (this == o) return true;
            if (o == null || getClass () != o.getClass ()) return false;
            Average average = (Average) o;
            return getSum () == average.getSum () &&
                    getCount () == average.getCount ();
        }

        @Override
        public int hashCode () {

            return Objects.hash (getSum (), getCount ());
        }

        @Override
        public String toString () {
            return "Average{" +
                    "sum=" + sum +
                    ", count=" + count +
                    '}';
        }
    }

    public static class MyTypedAverage extends Aggregator<Employee, Average, Double> {

        // A zero value for this aggregation. Should satisfy the property that any b + zero = b
        @Override
        public Average zero () {
            return new Average (0L, 0L);
        }

        // Combine two values to produce a new value. For performance, the function may modify `buffer`
        // and return it instead of constructing a new object
        @Override
        public Average reduce (Average buffer, Employee employee) {
            long newSum = buffer.getSum () + employee.getSalary ();
            long newCount = buffer.getCount () + 1;
            buffer.setSum (newSum);
            buffer.setCount (newCount);
            return buffer;
        }

        // Merge two intermediate values
        @Override
        public Average merge (Average b1, Average b2) {
            long mergedSum = b1.getSum () + b2.getSum ();
            long mergedCount = b1.getCount () + b2.getCount ();

            b1.setSum (mergedSum);
            b1.setCount (mergedCount);

            return b1;
        }

        // Transform the output of the reduction
        @Override
        public Double finish (Average reduction) {
            return ((double)reduction.getSum ()) / reduction.getCount ();
        }

        // Specifies the Encoder for the intermediate value type
        @Override
        public Encoder<Average> bufferEncoder () {
            return Encoders.bean (Average.class);
        }

        @Override
        public Encoder<Double> outputEncoder () {
            return Encoders.DOUBLE ();
        }
    }

    public static void main (String[] args) {

        SparkSession session = SparkSession
                .builder ()
                .appName ("Typed Average")
                .getOrCreate ();

        Encoder<Employee> employeeEncoder = Encoders.bean (Employee.class);
        String path = "data/employees.json";
        Dataset<Employee> ds = session.read ().json (path).as (employeeEncoder);
        ds.show ();
        /**
         +-------+------+
         |   name|salary|
         +-------+------+
         |Michael|  3000|
         |   Andy|  4500|
         | Justin|  3500|
         |  Berta|  4000|
         +-------+------+
         */
        MyTypedAverage myTypedAverage = new MyTypedAverage ();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myTypedAverage.toColumn ().name ("average_salary");
        Dataset<Double> result = ds.select (averageSalary);
        result.show ();
        /**
         +--------------+
         |average_salary|
         +--------------+
         |        3750.0|
         +--------------+
         */
    }
}
