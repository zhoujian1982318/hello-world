package com.examples.kafka.message.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseReceive {
	
	private final static Logger LOG = LoggerFactory.getLogger(BaseReceive.class);
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private  String group;
	private  List<String> topics;
	private  String receiveName;
	public BaseReceive(String name, String group, List<String> topics) {
		this.receiveName = name;
		this.group = group;
		this.topics = topics;
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.22.0.81:9093");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);
	}
	
	protected void subsribe() {
		consumer.subscribe(topics, new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				LOG.info("the partitions revoked method call");
				LOG.info("the partitions is {}", partitions);
				// consumer.seekToEnd(partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				LOG.info("the partitions assigned method call");
				LOG.info("the partitions is {}", partitions);
				// consumer.seekToEnd(partitions);
				// consumer.seekToBeginning(partitions);
			}
		});
		
	}
	
	protected void receiveMsg() {
		try {
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(2000);
				//LOG.info("return from poll...");
				for (ConsumerRecord<String, String> record : records) {
					LOG.info("group {}, receive name {} . Receive the message . the  message:, the key is {},  the msg is {}, the offset is {}, the partition is {}",
														group,  receiveName, record.key(), record.value(), record.offset(), record.partition());

				}
//				if (!records.isEmpty()) {
//					consumer.commitAsync(new OffsetCommitCallback() {
//						@Override
//						public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//							LOG.info("after commit the offsets is {}", offsets);
//						}
//					});
//				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
