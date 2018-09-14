package com.example.hadoop.hdfs;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.config.annotation.EnableDataStoreTextWriter;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreTextWriterConfigurerAdapter;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;

@Configuration
@EnableDataStoreTextWriter
public class DataStoreWriterConfig extends SpringDataStoreTextWriterConfigurerAdapter {

	@Override
	public void configure(DataStoreTextWriterConfigurer config) throws Exception {
		 config
	      .basePath("/content")
	      //.idleTimeout(60000)
	      //.closeTimeout(120000)
	      .inWritingSuffix(".tmp")
	      .withPartitionStrategy()
	      .custom(new ContentPartitionStrategy())
	      .and()
	      .withNamingStrategy()
	        .name("data")
	        .rolling()
	        .name("txt", ".")
	        .and()
	        .withRolloverStrategy()
	          .size("1M");
	}

	
	
}
