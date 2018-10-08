package com.examples.test;

public class TestLoad {

    static {
        System.out.println("TestLoad  init...");
    }

    public static void main(String[] args) {
        System.out.println(Constant.FINAL_INT_ONE);
        System.out.println(Constant.No_FINAL_INT);
        //System.out.println(Constant.FINAL_INTEGER_TWO);
        Object lock = new Object();
//        synchronized (lock){
//
//        }
    }
}
