package com.examples.spark.pojo;

import java.io.Serializable;

public class ClientCntInfo implements Serializable{

    private String apMac;
    private String clientMac;
    private int status;
    private String apName;

    public String getApMac() {
        return apMac;
    }

    public void setApMac(String apMac) {
        this.apMac = apMac;
    }

    public String getClientMac() {
        return clientMac;
    }

    public void setClientMac(String clientMac) {
        this.clientMac = clientMac;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getApName() {
        return apName;
    }

    public void setApName(String apName) {
        this.apName = apName;
    }

    @Override
    public String toString() {
         return String.format("%s,%s,%s,%d", apMac, apName, clientMac, status);
    }
}
