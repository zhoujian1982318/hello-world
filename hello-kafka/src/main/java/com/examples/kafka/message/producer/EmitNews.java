package com.examples.kafka.message.producer;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmitNews {
	private final static Logger LOG = LoggerFactory.getLogger(EmitNews.class);

	private final Random rnd = new Random();

	private final String[] newsTopic = new String[] { "sport",  "finance",  "ent",   "sport-photo", "ent-photo", "finance-photo" };

	private final KafkaProducer<String, String> producer;

	public EmitNews() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.22.0.81:9092");
		props.put("client.id", "newsProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	public static void main(String[] args) throws IOException {
		EmitNews emit = new EmitNews();
		
		for (int i=1; i<=10; i++) {
			String[] msgPair = emit.getMessage();
			emit.syncSendMessage(msgPair[0], String.valueOf(i),  msgPair[1]);
		}
	}
	
	/**
	 * kafka key 可以为 null, 默认kafka用 key 来选择 partition.  kafka.producer.DefaultPartitioner
	 * 可以通过配置 partitioner.class 来实现key 指定分区策略
	 * @param topic
	 * @param key
	 * @param msg
	 */
	private void syncSendMessage(String topic, String key, String msg) {

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, msg);
		try {
			RecordMetadata metaData = producer.send(record).get();
			LOG.info("topic : {} message key:{}, msg:{}. send to partition :{}", topic, key, msg, metaData.partition());
			LOG.info("timestamp of message is {}, the offset is :{}", metaData.timestamp(), metaData.offset());
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("send message failed!", e);
		}
	}
	private String[] getMessage() {
		int idx = rnd.nextInt(5);
		String topic = newsTopic[idx];
		String[] pairs = new String[2];
		switch (topic) {
			case "sport":
				pairs[0] = topic;
				pairs[1] = "the sport news, the random number is " + idx;
				break;
			case "finance":
				pairs[0] =  topic;
				pairs[1] = "the finance news, the random number is " + idx;
				break;
			case "ent":
				pairs[0] =  topic;
				pairs[1] = "the ent news, the random number is " + idx;
				break;
			case "sport-photo":
				pairs[0] =  topic;
				pairs[1] = "the random photo news, the random number is " + idx;
				break;
			case "ent-photo":
				pairs[0] =  topic;
				pairs[1] = "the random photo news, the random number is " + idx;
				break;
			case "finance-photo":
				pairs[0] =  topic;
				pairs[1] = "the random photo news, the random number is " + idx;
				break;
			default:
				break;
		}
		return pairs;
	}

}
