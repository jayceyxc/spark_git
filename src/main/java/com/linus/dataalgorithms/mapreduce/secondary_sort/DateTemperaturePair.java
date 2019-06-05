package com.linus.dataalgorithms.mapreduce.secondary_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The DateTemperaturePair class enable us to represent a
 * composite type of (yearMonth, day, temperature). To persist
 * a composite type (actually any data type) in Hadoop, it has
 * to implement the org.apache.hadoop.io.Writable interface.
 *
 * To compare composite types in Hadoop, it has to implement
 * the org.apache.hadoop.io.WritableComparable interface.
 *
 * @author Mahmoud Parsian
 *
 */

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private Text yearMonth = new Text ();  // Natural Key
    private Text day = new Text ();
    private IntWritable temperature = new IntWritable (); // Secondary Key

    public DateTemperaturePair() {

    }

    public DateTemperaturePair (String yearMonth, String day, int temperature) {
        this.yearMonth.set (yearMonth);
        this.day.set (day);
        this.temperature.set (temperature);
    }

    @Override
    /**
     * This comparator controls the sort order of the keys
     */
    public int compareTo (DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo (pair.getYearMonth ());
        if (compareValue == 0) {
            compareValue = this.temperature.compareTo (pair.getTemperature ());
        }

//        return compareValue; // sort ascending
        return -1 * compareValue; // sort descending
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair ();
        pair.readFields (in);
        return pair;
    }

    @Override
    public void write (DataOutput out) throws IOException {
        yearMonth.write (out);
        day.write (out);
        temperature.write (out);
    }

    @Override
    public void readFields (DataInput in) throws IOException {
        yearMonth.readFields (in);
        day.readFields (in);
        temperature.readFields (in);
    }

    public Text getYearMonthDay() {
        return new Text(yearMonth.toString () + day.toString ());
    }

    public Text getYearMonth () {
        return yearMonth;
    }

    public void setYearMonth (String yearMonth) {
        this.yearMonth.set (yearMonth);
    }

    public Text getDay () {
        return day;
    }

    public void setDay (String day) {
        this.day.set (day);
    }

    public IntWritable getTemperature () {
        return temperature;
    }

    public void setTemperature (int temperature) {
        this.temperature.set(temperature);
    }

    @Override
    public int hashCode () {
        int result = yearMonth != null ? yearMonth.hashCode () : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode () : 0);

        return result;
    }

    @Override
    public boolean equals (Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DateTemperaturePair that = (DateTemperaturePair) obj;
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }

        return true;
    }

    @Override
    public String toString () {
        StringBuilder builder = new StringBuilder ();
        builder.append ("DateTemperaturePair{yearMonth=");
        builder.append(yearMonth);
        builder.append(", day=");
        builder.append(day);
        builder.append(", temperature=");
        builder.append(temperature);
        builder.append("}");
        return builder.toString();
    }
}
