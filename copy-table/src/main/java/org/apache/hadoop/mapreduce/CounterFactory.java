package org.apache.hadoop.mapreduce;

public class CounterFactory {
    public static Counter createCounter(String s1, String s2) {
        return new Counter(s1, s2);
    }
}