package com.example.hadoop.pojo;

public class ClientStationInfo {

    private long id;
    private long acctId;
    private String apMac;
    private String bssid;
    private String ssid;
    private int radioProtocol;
    private int rssi;
    private int wlanId;
    private int radioSlum;
    private String cause;
    private long ts;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

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

    public String getBssid() {
        return bssid;
    }

    public void setBssid(String bssid) {
        this.bssid = bssid;
    }

    public String getSsid() {
        return ssid;
    }

    public void setSsid(String ssid) {
        this.ssid = ssid;
    }

    public int getRadioProtocol() {
        return radioProtocol;
    }

    public void setRadioProtocol(int radioProtocol) {
        this.radioProtocol = radioProtocol;
    }

    public int getRssi() {
        return rssi;
    }

    public void setRssi(int rssi) {
        this.rssi = rssi;
    }

    public int getWlanId() {
        return wlanId;
    }

    public void setWlanId(int wlanId) {
        this.wlanId = wlanId;
    }

    public int getRadioSlum() {
        return radioSlum;
    }

    public void setRadioSlum(int radioSlum) {
        this.radioSlum = radioSlum;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
