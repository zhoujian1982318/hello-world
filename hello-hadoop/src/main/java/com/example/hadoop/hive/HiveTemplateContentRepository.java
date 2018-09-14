package com.example.hadoop.hive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hive.HiveOperations;
import org.springframework.stereotype.Repository;

@Repository
public class HiveTemplateContentRepository {
	private static Logger logger = Logger.getLogger(HiveTemplateContentRepository.class);
	
	private @Value("${hive.table}") String tableName;
	
	private HiveOperations hiveOperations;
	
	@Autowired
	public HiveTemplateContentRepository(HiveOperations hiveOperations) {
		this.hiveOperations = hiveOperations;
	}
	
	public void loadContents(String inputFile) {
		Map<String,String> parameters = new HashMap<>();
		parameters.put("inputFile", inputFile);
		hiveOperations.query("classpath:content.hql", parameters);		
	}
	
	public void loadContentDetails(String inputFile) {
		Map<String,String> parameters = new HashMap<>();
		parameters.put("inputFile", inputFile);
		hiveOperations.query("classpath:content-detail.hql", parameters);		
	}
	
	public void loadExtContentDetails() {
		Map<String,String> parameters = new HashMap<>();
		hiveOperations.query("classpath:external-content-detail.hql", parameters);		
	}
	
	public Long count() {
		return hiveOperations.queryForLong("select count(*) from " + tableName);
	}
	
	public List<String> top10Hits() {
		
		List<String> list = hiveOperations.query("select content, hits, clen from content_stats limit 10");
		
		logger.info(String.format("top10 hits list is %s", list.toString()));
		
		return list;
	}
}
