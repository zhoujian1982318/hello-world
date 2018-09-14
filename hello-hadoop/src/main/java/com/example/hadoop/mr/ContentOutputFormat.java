package com.example.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class ContentOutputFormat extends MultipleTextOutputFormat<Text, ContentStats> {

	@Override
	protected String generateLeafFileName(String name) {
		return "content-result.txt";
	}

	
	

}
