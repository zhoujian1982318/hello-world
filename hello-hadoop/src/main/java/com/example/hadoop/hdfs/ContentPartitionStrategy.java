package com.example.hadoop.hdfs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.data.hadoop.store.partition.PartitionKeyResolver;
import org.springframework.data.hadoop.store.partition.PartitionResolver;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;

import com.example.hadoop.pojo.CollectedWebCacheUsage;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ContentPartitionStrategy implements PartitionStrategy<String, Long> {
	
	private static Logger logger = Logger.getLogger(ContentPartitionStrategy.class);
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	@Override
	public PartitionResolver<Long> getPartitionResolver() {
		
		return ( t-> {
			SimpleDateFormat sf =new SimpleDateFormat("yyyy/MM/dd");
			Date date = new Date(t);
			String path = sf.format(date);
			return new Path(path);
		});
	}

	@Override
	public PartitionKeyResolver<String, Long> getPartitionKeyResolver() {
		
		return  ( entity ->{
			try {
				CollectedWebCacheUsage usage = objectMapper.readValue(entity, CollectedWebCacheUsage.class);
				return usage.ts;
			} catch (IOException e) {
				logger.error(String.format("parser string [%s] to usage throw error", entity), e);
				return 0L ;
			}
		});
	}

}
