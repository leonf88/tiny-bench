package org.bench.util;

import org.bench.mergest.type.hadoop.FastByteComparisons;

import java.util.Random;

public class Utils {
    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
//    static SecureRandom rnd = new SecureRandom();
    public static Random rnd = new Random();

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    public static int compareBytes(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
        return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
    }
}
