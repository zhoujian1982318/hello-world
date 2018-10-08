package com.examples.test;

public class Constant {
    public static final int FINAL_INT_ONE = 1;

    public static final Integer FINAL_INTEGER_TWO  = 2;

    public static int No_FINAL_INT;

    static {
        System.out.println("static init...");
    }
}
