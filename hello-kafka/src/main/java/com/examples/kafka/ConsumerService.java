package com.examples.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerService  implements Runnable {
	
	  private final static Logger LOG = LoggerFactory.getLogger(ConsumerService.class);
	  private final AtomicBoolean closed = new AtomicBoolean(false);
	  private final KafkaConsumer<Integer, String> consumer;
	  private String name;
	  private String group;
	  private String topic;
	  
	  public ConsumerService(String group, String name, String topic, Map<String,String> pros) {
		  this.group = group;
		  this.name = name;
		  this .topic = topic;
		  Properties props = new Properties();
	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.22.0.81:9093");
	      props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
	      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	      if(pros!=null) {
	    	  props.putAll(pros);
	      }

	      consumer = new KafkaConsumer<Integer, String>(props);
	      
	  }
	  
//	  public void receiveMsgFromBegin() {
//	  		consumer.subscribe(Collections.singletonList(topic));
//	      	ConsumerRecords<Integer, String> records = consumer.poll(1000);
//	      	for (ConsumerRecord<Integer, String> record : records) {
//	    	  LOG.info("Group {}, name {} . Receive the message . the  message:, the key is {},  the msg is {}, the offset is {}, the partition is {}", 
//	    			  	group, name,record.key() , record.value() , record.offset(), record.partition());
//	      	}
//	  }
	  
		@Override
		public void run() {
			try {
			 		consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

						@Override
						public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
							LOG.info("the partitions revoked method call");
							LOG.info("the partitions is {}", partitions);
							//consumer.seekToEnd(partitions);
							
						}

						@Override
						public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
							LOG.info("the partitions assigned method call");
					 		LOG.info("the partitions is {}", partitions);
					 		//consumer.seekToEnd(partitions);
					 		//consumer.seekToBeginning(partitions);
							
						}
			 		});
			 		
//			 	   consumer.seekToEnd(partitions);
//			 	   Map<TopicPartition, Long> offSets =  consumer.endOffsets(partitions);
//			 	   LOG.info("the offSets is {}", offSets);
		            while (!closed.get()) {
		               try {
			   		      	ConsumerRecords<Integer, String> records = consumer.poll(2000);
//			   		      	LOG.info("return from poll...");
			   		      	for (ConsumerRecord<Integer, String> record : records) {
			   		    	  LOG.info("Group {}, name {} . Receive the message . the  message:, the key is {},  the msg is {}, the offset is {}, the partition is {}", 
			   		    			  	group, name,record.key() , record.value() , record.offset(), record.partition());
			   		    	  
			   		      	}
//			   		      	if(!records.isEmpty()) {
//				   		      	consumer.commitAsync(new OffsetCommitCallback() {
//									@Override
//									public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//										LOG.info("after commit the offsets is {}", offsets);
//									}
//								});
//			   		      	}
		            	}catch(Exception e) {
							e.printStackTrace();
						}
		            }
				
	        } catch (WakeupException e) {
	            // Ignore exception if closing
	            if (!closed.get()) throw e;
	        } finally {
	            consumer.close();
	        }
			
		}
		
		public static void main(String[] args) {
			
//			ConsumerService cnsumer1Grp1 = new  ConsumerService("group1", "name1","test");
			ConsumerService cnsumer2Grp1 = new  ConsumerService("group1", "name3", "test", null);
			
			ConsumerService cnsumer1Grp2 = new  ConsumerService("group2", "name2", "test", null);
			
//			Thread c1 =new Thread(cnsumer1Grp1);
			Thread c2 =new Thread(cnsumer2Grp1);
			Thread c3 =new Thread(cnsumer1Grp2);
//			c1.start();
			c2.start();
			c3.start();
			
			try {
				BufferedReader   reader=new   BufferedReader(new   InputStreamReader(System.in)); 
				reader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
}
