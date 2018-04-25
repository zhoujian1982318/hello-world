package com.examples.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService {

	private final static Logger LOG = LoggerFactory.getLogger(ProducerService.class);

	private final KafkaProducer<Integer, String> producer;

	public ProducerService() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.22.0.81:9092");
		props.put("client.id", "DemoProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	/**
	 * kafka key 可以为 null, 默认kafka用 key 来选择 partition.  kafka.producer.DefaultPartitioner
	 * 可以通过配置 partitioner.class 来实现key 指定分区策略
	 * @param topic
	 * @param key
	 * @param msg
	 */
	public void syncSendMessage(String topic, Integer key, String msg) {

		ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, key, msg);
		try {
			RecordMetadata metaData = producer.send(record).get();
			LOG.info("message key:{}, msg:{}. send to partition :{}", key, msg, metaData.partition());
			LOG.info("timestamp of message is {}, the offset is :{}", metaData.timestamp(), metaData.offset());
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("send message failed!", e);
		}
	}

	public void asyncSendMessage(String topic, Integer key, String msg) {

		ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, key, msg);
		long startTime = System.currentTimeMillis();
		producer.send(record, new DemoCallBack(startTime, key, msg));
		LOG.info("async send Message, the thread can do other work.");
	}
	
	
	public void getMetrics() {
		LOG.info("the metric of producer is {}",producer.metrics());
	}

	class DemoCallBack implements Callback {

		private final long startTime;
		private final int key;
		private final String message;

		public DemoCallBack(long startTime, int key, String message) {
			this.startTime = startTime;
			this.key = key;
			this.message = message;
		}

		/**
		 * A callback method the user can implement to provide asynchronous handling of
		 * request completion. This method will be called when the record sent to the
		 * server has been acknowledged. Exactly one of the arguments will be non-null.
		 *
		 * @param metadata
		 *            The metadata for the record that was sent (i.e. the partition and
		 *            offset). Null if an error occurred.
		 * @param exception
		 *            The exception thrown during processing of this record. Null if no
		 *            error occurred.
		 */
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			if (metadata != null) {
				LOG.info("message key:{}, msg:{}. send to partition :{}", key, message, metadata.partition());
				LOG.info("timestamp of message is {}, the offset is :{}", metadata.timestamp(), metadata.offset());
				LOG.info("elapsedTime time is {} ms ", elapsedTime);
			} else {
				exception.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		ProducerService producerService = new ProducerService();
		String[] msg = new String[]{
				"Note that the data is being stored in the Kafka topi",
				"Kafka Streams is a client library for building mission-critical real-time applications and microservices",
				"custom consumer code to process it",
				"elapsedTime time is",
				"in Kafka clusters. Kafka Streams combines the simplicity of writing and deploying standard Java and Scala applications on the client side",
				"Kafka Streams is a client library",
				"custom consumer code to process",
				"in Kafka clusters",
				"in Kafka clusters",
				"custom consumer code to process it"
		};
		
		while(true) {
			for(int i=0; i<10; i++) {
				producerService.syncSendMessage("words-test",i , msg[i]);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
