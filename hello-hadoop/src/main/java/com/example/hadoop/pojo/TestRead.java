package com.example.hadoop.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestRead {

	public static void main(String[] args) throws JsonProcessingException {
//		ObjectMapper obj =  new ObjectMapper();
//		CollectedWebCacheUsage usg = new CollectedWebCacheUsage();
//		String tmp = obj.writeValueAsString(usg);
//		System.out.println(tmp);
		String txt= "5C:A8:6A:ED:65:2D,B4:82:C5:00:22:DB,B4:82:C5:51:C2:30,\"UREC\",2097154,4,14,-59,6,2,Deauth by Inactivity or Many Retries,1517391829738";
		String[] spliturl = txt.split(",");
		System.out.println(spliturl[0]);
		System.out.println(spliturl[4]);
		System.out.println(spliturl[5]);
	}

}
