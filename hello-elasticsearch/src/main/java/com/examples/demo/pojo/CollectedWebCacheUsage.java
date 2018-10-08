package com.examples.demo.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonRootName("UrAl")
public class CollectedWebCacheUsage {


    private long ts;
    private long ttl;
    private String aMc;
    private long usd;
    private long avl;
    public List<WebCacheStat> wcStats = new ArrayList<>();

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public void setaMc(String aMc) {
        this.aMc = aMc;
    }

    public void setUsd(long usd) {
        this.usd = usd;
    }

    public void setAvl(long avl) {
        this.avl = avl;
    }

    public void setWcStats(List<WebCacheStat> wcStats) {
        this.wcStats = wcStats;
    }

    public long getTs() {
        return ts;
    }

    public long getTtl() {
        return ttl;
    }

    public String getaMc() {
        return aMc;
    }

    public long getUsd() {
        return usd;
    }

    public long getAvl() {
        return avl;
    }

    public List<WebCacheStat> getWcStats() {
        return wcStats;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class WebCacheStat {

        private String url;
        private long clen;
        private int wid;
        private int hits;
        private int misses;
        private int bypasses;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public long getClen() {
            return clen;
        }

        public void setClen(long clen) {
            this.clen = clen;
        }

        public int getWid() {
            return wid;
        }

        public void setWid(int wid) {
            this.wid = wid;
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

        public int getBypasses() {
            return bypasses;
        }

        public void setBypasses(int bypasses) {
            this.bypasses = bypasses;
        }
    }
}
