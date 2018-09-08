package com.examples.spark.pojo;

import scala.Tuple2;

public class ClinetCntCompartor implements  Comparable<Tuple2<Long, Integer>> {


    @Override
    public int compareTo(Tuple2<Long, Integer> o) {
        return 0;
    }
}
