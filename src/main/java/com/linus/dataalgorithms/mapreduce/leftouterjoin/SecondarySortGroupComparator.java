package com.linus.dataalgorithms.mapreduce.leftouterjoin;

import com.linus.dataalgorithms.utils.Pair;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

public class SecondarySortGroupComparator implements RawComparator<Pair<String, String>>{
    @Override
    public int compare (byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        DataInputBuffer buffer = new DataInputBuffer ();
        Pair<String, String> a = new Pair<String, String> ("a", "b");
        return 0;
    }

    @Override
    public int compare (Pair<String, String> o1, Pair<String, String> o2) {
        return o1.getLeft ().compareTo (o2.getLeft ());
    }
}
