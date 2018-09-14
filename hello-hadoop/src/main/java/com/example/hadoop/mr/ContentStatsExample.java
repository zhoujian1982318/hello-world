package com.example.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.slf4j.Logger;
import org.apache.log4j.Logger;
//import org.slf4j.LoggerFactory;

import com.example.hadoop.pojo.CollectedWebCacheUsage;
import com.fasterxml.jackson.databind.ObjectMapper;



public class ContentStatsExample {
	
	private static Logger logger = Logger.getLogger(ContentStatsExample.class);
	//private static Logger logger = LoggerFactory.getLogger(ContentStatsExample.class);
	
	public static class ContentMapper extends Mapper<Object, Text, Text, ContentStats>{
		
		private static ObjectMapper objectMapper = new ObjectMapper();
		
		private Text content = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String jsonStr = value.toString();
			logger.debug("the json string is {" + jsonStr + "}");
			CollectedWebCacheUsage usage = objectMapper.readValue(jsonStr, CollectedWebCacheUsage.class);
			System.out.println("usage is {" + usage.toString() + "}");
			for (CollectedWebCacheUsage.WebCacheStat cacheStat : usage.wcStats) {
				 String url = cacheStat.url;
	                if(url==null || "".equals(url)){
	                	logger.error("the url must not be null, ignore this item : [" + cacheStat +"]");
	                    continue;
	                }
	                String cName = parseWindowContentName(url);
	                ContentStats cSts = new ContentStats(cacheStat.hits, cacheStat.clen);
	                content.set(cName);
	                logger.info("parser the content name : {" + cName+"} hits: ["+ cacheStat.hits +"]");
	                context.write(content, cSts);
			} 
		}
		

	    private String parseWindowContentName(String url) {
	        int pos = url.lastIndexOf("/");
	        String name = "";
	        if(pos > 0 ){
	            name = url.substring(pos+1);
	        }
	        return name;
	    }
	}
	
	
	public static class ContentStatsReducer extends Reducer<Text, ContentStats, Text, ContentStats>{

		@Override
		protected void reduce(Text contName, Iterable<ContentStats> stats,
				Context context) throws IOException, InterruptedException {
			logger.info("the contentName is {" + contName.toString() + "}");
			int hits = 0;
			long size = 0L;
			for(ContentStats stat : stats) {
				hits+=stat.getHits();
				if(stat.getSize()!=0) {
					size = stat.getSize();
				}
			}
			ContentStats contSts = new ContentStats(hits, size);
			context.write(contName, contSts);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//设置MapReduce的输出的分隔符为逗号
		//conf.set("mapred.textoutputformat.ignoreseparator", "true");
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in>  <out>");
			System.exit(2);
		}
		
		System.out.println("init the job instance");
		
		Job job = Job.getInstance(conf, "content stats example");
		job.setJarByClass(ContentStatsExample.class);
		job.setMapperClass(ContentMapper.class);
		job.setCombinerClass(ContentStatsReducer.class);
		job.setReducerClass(ContentStatsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ContentStats.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
