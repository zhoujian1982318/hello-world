package com.example.hadoop.hive;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hive.HiveTemplate;

public class HiveApplication {
	
	private static Logger logger = Logger.getLogger(HiveApplication.class);
	
	public static void main(String[] args) {
		
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"/spring-hive-context.xml", HiveApplication.class);
		logger.info("Hive Application Running");
		context.registerShutdownHook();
		
		HiveTemplate template = context.getBean(HiveTemplate.class);
		List<String> list = template.query("show tables;");
		
		logger.info("the table is :" + list);

		HiveTemplateContentRepository repository = context.getBean(HiveTemplateContentRepository.class);
		//repository.loadContents("/content/data-5.txt");
		
		repository.loadExtContentDetails();
		
		repository.top10Hits();
		
		//long count = repository.count();
		
		//logger.info(String.format("the count of contents is  %d", count));
		
		context.close();
	}
}
