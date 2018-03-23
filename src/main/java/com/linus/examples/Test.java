package com.linus.examples;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class Test {

    /**
     *
     * @param tokens 用户ID列表，第一个表示自己，后面的表示好友
     * @return 逗号分割的好友列表
     */
    private static String buildFriendsString(String[] tokens) {
        if (tokens.length == 1) {
            return "";
        } else if (tokens.length == 2) {
            return tokens[1];
        } else {
            String[] friends = Arrays.copyOfRange (tokens, 1, tokens.length);
            return StringUtils.join (friends, ",");
        }
    }

    public static void main (String[] args) {
        String[] tokens = "100 200 300 400".split (" ");
        System.out.println (buildFriendsString (tokens));
    }
}
