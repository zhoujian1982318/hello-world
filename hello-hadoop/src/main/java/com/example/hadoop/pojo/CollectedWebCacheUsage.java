package com.example.hadoop.pojo;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "type")
@JsonTypeName("UrAl") 
public class CollectedWebCacheUsage {
	public long ts;
	public long ttl;
	public String aMc;
	public String apName;
	public long usd;
	public long avl;
	public List<WebCacheStat> wcStats = new ArrayList<>();
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class WebCacheStat{
		public String url;
		public long clen;
		public int wid;
		public int hits;
		public int misses;
		public int bypasses;
		public long hbytes; //add since 1.9.2
		public long mbytes; //add since 1.9.2
		public long bbytes; //add since 1.9.2
		public String usvr = ""; //add since 1.9.2
		public int errors; //add since 1.9.2
		public long cdate; //add since 1.9.2
		@JsonIgnore
		public int sid; // space id

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + sid;
			result = prime * result + ((url == null) ? 0 : url.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			WebCacheStat other = (WebCacheStat) obj;
			if (sid != other.sid)
				return false;
			if (url == null) {
				if (other.url != null)
					return false;
			} else if (!url.equals(other.url))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "WebCacheStat [url=" + url + ", clen=" + clen + ", wid=" + wid + ", hits=" + hits + ", misses="
					+ misses + ", bypasses=" + bypasses + "]";
		}
	}

	@Override
	public String toString() {
		return "CollectedWebCacheUsage [ts=" + ts + ", ttl=" + ttl + ", aMc=" + aMc + ", apName=" + apName + ", usd=" + usd + ", avl=" + avl
				+ ", wcStats=" + wcStats + "]";
	}
}
