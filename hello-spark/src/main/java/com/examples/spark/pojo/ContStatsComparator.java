package com.examples.spark.pojo;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class ContStatsComparator implements Comparator<Tuple2<String, ContentStats>>, Serializable {

    @Override
    public int compare(Tuple2<String, ContentStats> o1, Tuple2<String, ContentStats> o2) {
        return  o2._2.getHits() - o1._2.getHits() ;
    }
}
