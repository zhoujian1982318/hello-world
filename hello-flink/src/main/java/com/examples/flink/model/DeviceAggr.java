package com.examples.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class DeviceAggr {
	
	@JsonProperty("did")
	private Integer deviceId;
	
	@JsonProperty("aid")
	private Integer appId;
	
	@JsonProperty("ub")
	private long upBytes;
	@JsonProperty("db")
	private long downBytes;
	
	@JsonProperty("aggTs")
	private Long aggTs;
	
	@JsonProperty("aggTime")
	private String aggTime;

	
	public String getAggTime() {
		return aggTime;
	}

	public void setAggTime(String aggTime) {
		this.aggTime = aggTime;
	}

	public Integer getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(Integer deviceId) {
		this.deviceId = deviceId;
	}

	public Integer getAppId() {
		return appId;
	}

	public void setAppId(Integer appId) {
		this.appId = appId;
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

	public Long getAggTs() {
		return aggTs;
	}

	public void setAggTs(Long aggTs) {
		this.aggTs = aggTs;
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("deviceId", deviceId)
				.add("appId", appId).add("upBytes", upBytes)
				.add("downBytes", downBytes)
				.add("aggTs", aggTs).add("aggTime", aggTime).toString();
	}
}
