package com.hazelcast;

import java.util.Random;

/**
 * Created by alarmnummer on 4/5/14.
 */
public class Util {
    public static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int k = 0; k < length; k++) {
            char c = (char) random.nextInt(Character.MAX_VALUE);
            sb.append(c);
        }
        return sb.toString();
    }
}
