package com.examples.demo.pojo;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("apCache")
public class ApCacheData {

    private long acctId;
    private String apMac;
    private int hits;
    private int misses;
    private long size;
    private long ts;
    private String cName;
    public long getTs() {
        return ts;
    }
    public void setTs(long ts) {
        this.ts = ts;
    }
    public String getApMac() {
        return apMac;
    }
    public void setApMac(String apMac) {
        this.apMac = apMac;
    }

    public int getHits() {
        return hits;
    }
    public void setHits(int hits) {
        this.hits = hits;
    }

    public int getMisses() {
        return misses;
    }

    public void setMisses(int misses) {
        this.misses = misses;
    }

    public long getAcctId() {
        return acctId;
    }

    public void setAcctId(long acctId) {
        this.acctId = acctId;
    }

    public String getcName() {
        return cName;
    }
    public void setcName(String cName) {
        this.cName = cName;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
