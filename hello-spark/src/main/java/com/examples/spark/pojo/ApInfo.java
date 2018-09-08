package com.examples.spark.pojo;

import java.io.Serializable;

public class ApInfo implements Serializable {

    private long acctId;
    private String apMac;
    private String apName;
    private String acctName;

    public long getAcctId() {
        return acctId;
    }

    public void setAcctId(long acctId) {
        this.acctId = acctId;
    }

    public String getApMac() {
        return apMac;
    }

    public void setApMac(String apMac) {
        this.apMac = apMac;
    }

    public String getApName() {
        return apName;
    }

    public void setApName(String apName) {
        this.apName = apName;
    }

    public String getAcctName() {
        return acctName;
    }

    public void setAcctName(String acctName) {
        this.acctName = acctName;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%d,%s", apMac, apName, acctId, acctName);
    }
}
