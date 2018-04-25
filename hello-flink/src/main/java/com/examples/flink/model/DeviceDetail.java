package com.examples.flink.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceDetail {
	
	private static Random rnd = new Random();
	private  static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@JsonProperty("did")
	private Integer deviceId;
	@JsonProperty("mac")
	private String mac;
	@JsonProperty("aid")
	private Integer appId;
	@JsonProperty("anm")
	private String appName;
	@JsonProperty("ub")
	private long upBytes;
	@JsonProperty("db")
	private long downBytes;
	@JsonProperty("ets")
	private Long eventTs;
	@JsonProperty("eventTime")
	private String eventTime;
	
	public DeviceDetail() {
		
	}
	
	public DeviceDetail(Integer did, String mac, Integer appId) {
		deviceId = did;
		this.mac = mac;
		this.appId = appId;
		this.appName = mac+"-name-"+appId;
		this.upBytes = rnd.nextInt(100);
		this.downBytes = rnd.nextInt(100);
		this.eventTs = 	 randomDate("2018-04-17 8:00:00", "2018-04-17 18:00:00");
		this.eventTime = format.format(eventTs);
	}
	
	
	public DeviceDetail(Integer did, String mac, Integer appId, long eventTs) {
		deviceId = did;
		this.mac = mac;
		this.appId = appId;
		this.appName = mac+"-name-"+appId;
		this.upBytes = rnd.nextInt(100);
		this.downBytes = rnd.nextInt(100);
		this.eventTs = eventTs;
		this.eventTime = format.format(eventTs);
	}
	
	public Integer getAppId() {
		return appId;
	}
	public void setAppId(Integer appId) {
		this.appId = appId;
	}
	public String getAppName() {
		return appName;
	}
	public void setAppName(String appName) {
		this.appName = appName;
	}
	public long getUpBytes() {
		return upBytes;
	}
	public void setUpBytes(long upBytes) {
		this.upBytes = upBytes;
	}
	public long getDownBytes() {
		return downBytes;
	}
	public void setDownBytes(long downBytes) {
		this.downBytes = downBytes;
	}
	public Long getEventTs() {
		return eventTs;
	}
	public void setEventTs(Long eventTs) {
		this.eventTs = eventTs;
	}
	public Integer getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(Integer deviceId) {
		this.deviceId = deviceId;
	}
	public String getMac() {
		return mac;
	}
	public void setMac(String mac) {
		this.mac = mac;
	}
	
	 /** 
     * 获取随机日期 
     * @param beginDate 起始日期，格式为：yyyy-MM-dd 
     * @param endDate 结束日期，格式为：yyyy-MM-dd 
     * @return 
     */  
    private  static Long randomDate(String beginDate,String endDate){  
        try {  
            Date start = format.parse(beginDate);  
            Date end = format.parse(endDate);  
              
            if(start.getTime() >= end.getTime()){  
                return null;  
            }  
              
            long date = random(start.getTime(),end.getTime());  
              
            return date;  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        return null;  
    }  
      
    public static Random getRnd() {
		return rnd;
	}

	public static void setRnd(Random rnd) {
		DeviceDetail.rnd = rnd;
	}

	public static SimpleDateFormat getFormat() {
		return format;
	}

	public static void setFormat(SimpleDateFormat format) {
		DeviceDetail.format = format;
	}

	private static long random(long begin,long end){  
        long rtn = begin + (long)(Math.random() * (end - begin));  
        if(rtn == begin || rtn == end){  
            return random(begin,end);  
        }  
        return rtn;  
    }

	public String getEventTime() {
		return eventTime;
	}

	public void setEventTime(String eventTime) {
		this.eventTime = eventTime;
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("deviceId", deviceId)
						.add("appId", appId).add("upBytes", upBytes)
						.add("downBytes", downBytes)
						.add("eventTime", eventTime).toString();
	}

}
