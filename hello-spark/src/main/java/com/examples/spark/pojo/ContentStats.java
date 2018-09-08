package com.examples.spark.pojo;

import java.io.Serializable;

public class ContentStats implements Serializable {

    private int hits;

    private long size;

    public ContentStats() {
    }

    public ContentStats(int hits, long size) {
        this.hits = hits;
        this.size = size;
    }

    public int getHits() {
        return hits;
    }


    public void setHits(int hits) {
        this.hits = hits;
    }


    public long getSize() {
        return size;
    }


    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "" + hits +  "," + size + "";
    }


}
