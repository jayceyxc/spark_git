package com.linus.dataalgorithms.mapreduce.order_inversion;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairOfWords implements WritableComparable<PairOfWords> {
    private String leftElement;
    private String rightElement;

    /**
     * Creates a pair.
     */
    public PairOfWords()
    {

    }

    /**
     * Creates a pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public PairOfWords(String left, String right)
    {
        set (left, right);
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public void write (DataOutput out) throws IOException {
        Text.writeString (out, leftElement);
        Text.writeString (out, rightElement);
    }

    /**
     * Deserializes the pair.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields (DataInput in) throws IOException {
        leftElement = Text.readString (in);
        rightElement = Text.readString (in);
    }

    public void setLeftElement(String leftElement) {
        this.leftElement = leftElement;
    }

    public void setWord(String leftElement) {
        setLeftElement(leftElement);
    }

    /**
     * Returns the leftElement element.
     *
     * @return the leftElement element
     */
    public String getWord() {
        return leftElement;
    }

    /**
     * Returns the leftElement element.
     *
     * @return the leftElement element
     */
    public String getLeftElement() {
        return leftElement;
    }

    public void setRightElement(String rightElement) {
        this.rightElement = rightElement;
    }

    public void setNeighbor(String rightElement) {
        setRightElement(rightElement);
    }

    /**
     * Returns the rightElement element.
     *
     * @return the rightElement element
     */
    public String getRightElement() {
        return rightElement;
    }

    public String getNeighbor() {
        return rightElement;
    }

    /**
     * Returns the key (leftElement element).
     *
     * @return the key
     */
    public String getKey() {
        return leftElement;
    }

    /**
     * Returns the value (rightElement element).
     *
     * @return the value
     */
    public String getValue() {
        return rightElement;
    }

    /**
     * Sets the rightElement and leftElement elements of this pair.
     *
     * @param left the leftElement element
     * @param right the rightElement element
     */
    public void set(String left, String right) {
        leftElement = left;
        rightElement = right;
    }

    /**
     * Defines a natural sort order for pairs. Pairs are sorted first by the left element, and then by the right
     * element.
     *
     * @return a value less than zero, a value greater than zero, or zero if this pair should be sorted before, sorted
     * after, or is equal to <code>obj</code>.
     */
    @Override
    public int compareTo(PairOfWords pair) {
        String pl = pair.getLeftElement();
        String pr = pair.getRightElement();

        if (leftElement.equals(pl)) {
            return rightElement.compareTo(pr);
        }

        return leftElement.compareTo(pl);
    }

    /**
     * Checks two pairs for equality.
     *
     * @param o object for comparison
     * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code> otherwise
     */
    @Override
    public boolean equals (Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof PairOfWords)) {
            return false;
        }
        PairOfWords that = (PairOfWords) o;
        return Objects.equals (leftElement, that.leftElement) &&
                Objects.equals (rightElement, that.rightElement);
    }

    @Override
    public int hashCode () {

        return Objects.hash (leftElement, rightElement);
    }

    @Override
    public String toString () {
        return "PairOfWords{" +
                "leftElement='" + leftElement + '\'' +
                ", rightElement='" + rightElement + '\'' +
                '}';
    }

    @Override
    protected Object clone () throws CloneNotSupportedException {
        return new PairOfWords (this.leftElement, this.rightElement);
    }

    /**
     * Comparator optimized for <code>PairOfWords</code>.
     */
    public static class Comparator extends WritableComparator {

        /**
         * Creates a new Comparator optimized for <code>PairOfWords</code>.
         */
        public Comparator() {
            super(PairOfWords.class);
        }

        /**
         * Optimization hook.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
                int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
                int firstStrL1 = readVInt(b1, s1);
                int firstStrL2 = readVInt(b2, s2);
                int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1, b2, s2 + firstVIntL2, firstStrL2);
                if (cmp != 0) {
                    return cmp;
                }

                int secondVIntL1 = WritableUtils.decodeVIntSize(b1[s1 + firstVIntL1 + firstStrL1]);
                int secondVIntL2 = WritableUtils.decodeVIntSize(b2[s2 + firstVIntL2 + firstStrL2]);
                int secondStrL1 = readVInt(b1, s1 + firstVIntL1 + firstStrL1);
                int secondStrL2 = readVInt(b2, s2 + firstVIntL2 + firstStrL2);
                return compareBytes(b1, s1 + firstVIntL1 + firstStrL1 + secondVIntL1, secondStrL1, b2,
                        s2 + firstVIntL2 + firstStrL2 + secondVIntL2, secondStrL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static { // register this comparator
        WritableComparator.define(PairOfWords.class, new Comparator());
    }
}
