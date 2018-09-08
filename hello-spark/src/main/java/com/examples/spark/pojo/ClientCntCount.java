package com.examples.spark.pojo;

import java.io.Serializable;

public class ClientCntCount implements Serializable, Comparable<ClientCntCount> {

    private long acctId;
    private int count;
    private String clientMac;



    public ClientCntCount(){};

    public long getAcctId() {
        return acctId;
    }

    public void setAcctId(long acctId) {
        this.acctId = acctId;
    }

    public ClientCntCount(long acctId, int count){
        this.acctId = acctId;
        this.count = count;
    }

    public ClientCntCount(String clientMac, long acctId, int count){
        this.clientMac = clientMac;
        this.acctId = acctId;
        this.count = count;
    }

    public ClientCntCount(String clientMac, int count){
        this.clientMac = clientMac;
        this.count = count;
    }

    public String getClientMac() {
        return clientMac;
    }

    public void setClientMac(String clientMac) {
        this.clientMac = clientMac;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }


    @Override
    public int compareTo(ClientCntCount o) {
        return Long.compare(acctId,o.acctId);
    }


    @Override
    public String toString() {
        return String.format("%d,%s, %d", acctId, clientMac,  count);
    }
}
