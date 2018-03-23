package com.linus.dataalgorithms.mapreduce.common_friends;

import java.util.Set;
import java.util.TreeSet;

public class CommonFriends {
    // user1friends = {A1, A2, A3, ..., Am}
    // user2friends = {B1, B2, B3, ..., Bn}
    public static Set<Integer> intersection(Set<Integer> user1friends,
                                            Set<Integer> user2friends) {
        if ((user1friends == null) || user1friends.isEmpty ()) {
            return null;
        }

        if ((user2friends == null) || user2friends.isEmpty ()) {
            return null;
        }

        // 两个集合都非null
        if (user1friends.size () < user2friends.size ()) {
            return intersect (user1friends, user2friends);
        } else {
            return intersect (user2friends, user1friends);
        }
    }

    private static Set<Integer> intersect(Set<Integer> smallSet,
                                          Set<Integer> largeSet) {
        Set<Integer> result = new TreeSet<Integer> ();
        // 迭代处理小集合来提高性能
        for (Integer x : smallSet) {
            if (largeSet.contains (x)) {
                result.add (x);
            }
        }

        return result;
    }

    public static void main (String[] args) {
        Set<Integer> user1Friends = new TreeSet<> ();
        user1Friends.add (100);
        user1Friends.add (200);
        user1Friends.add (500);
        Set<Integer> user2Friends = new TreeSet<> ();
        user2Friends.add (100);
        user2Friends.add (300);
        user2Friends.add (500);
        user2Friends.add (700);

        Set<Integer> commonSet = intersection (user1Friends, user2Friends);
        for (Integer x : commonSet) {
            System.out.println (x);
        }
    }
}
