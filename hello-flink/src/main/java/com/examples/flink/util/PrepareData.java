package com.examples.flink.util;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.DeviceDetail;
import com.examples.flink.stream.DeviceJob;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PrepareData {
	
	private static Logger LOG = LoggerFactory.getLogger(PrepareData.class);

	private static ObjectMapper mapper = new ObjectMapper();
	
	private final KafkaProducer<Object, String> producer;
	
	public PrepareData() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.22.0.81:9092");
		props.put("client.id", "DeviceDataProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	public static void main(String[] args) {

		//prepareOrderData();
		
		prepareLatenessData();
		
		//prepareStateData();
		
		//sendDataToKafka();
		
		 //prepareBookOrderData();
		 //preparePhoneOrderData();
	}

	private static void preparePhoneOrderData() {
		final String[] PHONES = {"Samsung", "IPhone", "XiaoMi", "HuaWei", "Oppo"};
		final Random rnd = new Random();
		try {
			FileWriter out = new FileWriter("PhoneOrderInput.txt");
			String username="Jason";
			for (int i = 0; i < 5; i++) {
				if(i>2) {
					username = "zml";
				}
				String text = String.format("%s,%s,%d,%d", username, PHONES[i],  rnd.nextInt(100), rnd.nextInt(5) ) ;
				out.write(text);
				out.write(System.lineSeparator());
			}
			 out.flush();
			 out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void prepareBookOrderData() {
		
		final String[] BOOKS = {"Effictive Java", "Flink In Action", "Kafka In Action", "Masting Java Script", "Spring in Action"};
		final Random rnd = new Random();
		try {
			FileWriter out = new FileWriter("BookOrderInput.txt");
			String username="Jason";
			for (int i = 0; i < 5; i++) {
				if(i>2) {
					username = "zml";
				}
				String text = String.format("%s,%s,%d,%d", username, BOOKS[i],  rnd.nextInt(100), rnd.nextInt(5)) ;
				out.write(text);
				out.write(System.lineSeparator());
			}
			 out.flush();
			 out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void sendDataToKafka() {
		
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			PrepareData data = new PrepareData();
			
			Date time = format.parse("2018-04-18 00:00:00");
			Calendar gc = Calendar.getInstance();
			gc.setTime(time);

			int key= 1;
			for (int i = 0; i < 1000000000; i++) {
				int tmp = key % 2;
				int appId = 100+tmp;
				DeviceDetail detail = new DeviceDetail(1,"B4:82:45:00:00:01", appId,  gc.getTime().getTime());
				gc.add(Calendar.MINUTE, 10);
				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
				String jsonStr = mapper.writeValueAsString(detail);
//				out.write(jsonStr);
//				out.write(System.lineSeparator());
				data.syncSendMessage("device-data", key, jsonStr);
				key++;
				Thread.sleep(5000);
			}
			
			
//			// generate 2 data after 12:00
//			time = format.parse("2018-04-17 12:20:00");
//			gc.setTime(time);
//			for (int i = 0; i < 2; i++) {
//				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01",101,gc.getTime().getTime());
//				gc.add(Calendar.MINUTE, 30);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				String jsonStr = mapper.writeValueAsString(detail);
//				data.syncSendMessage("device-data", key, jsonStr);
////				out.write(jsonStr);
////				out.write(System.lineSeparator());
//				key++;
//			}
//			
//			// generate 2 data after 18:00
//			time = format.parse("2018-04-17 18:20:00");
//			gc.setTime(time);
//			for (int i = 0; i < 2; i++) {
//				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01",101,gc.getTime().getTime());
//				gc.add(Calendar.MINUTE, 30);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				String jsonStr = mapper.writeValueAsString(detail);
//				data.syncSendMessage("device-data", key, jsonStr);
//				key++;
//			}
//			// generate 2 data befor 12:00
//			//sleep 1 minutes. and generate 2 data before 12:00
//			Thread.sleep(60*1000);
//			
//			time = format.parse("2018-04-17 08:00:00");
//			gc.setTime(time);
//			for (int i = 0; i < 2; i++) {
//				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01",101,gc.getTime().getTime());
//				gc.add(Calendar.MINUTE, 30);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				String jsonStr = mapper.writeValueAsString(detail);
//				data.syncSendMessage("device-data", key, jsonStr);
//				key++;
//			}
//			out.flush();
//			out.close();
			
		} catch (IOException | ParseException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	private static void prepareStateData() {
		try {
			FileWriter out = new FileWriter("keystate.txt");
			for (int i = 1; i < 10; i++) {
				String text = "key"+","+i;
				out.write(text);
				out.write(System.lineSeparator());
			}
			out.flush();
			out.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void prepareLatenessData() {
		
		try {
//			FileWriter out = new FileWriter("deviceInputLateness.txt");
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			PrepareData data = new PrepareData();
			
			Date time = format.parse("2018-04-25 10:00:00");
			Calendar gc = Calendar.getInstance();
			gc.setTime(time);
			// SimpleDateFormat
			// generate  2 data before 12:00
			int key= 1;
			for (int i = 0; i < 2; i++) {
				int tmp = key % 2;
				int appId = 100+tmp;
				DeviceDetail detail = new DeviceDetail(1,"B4:82:45:00:00:01", 101,  gc.getTime().getTime());
				gc.add(Calendar.MINUTE, 10);
				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
				String jsonStr = mapper.writeValueAsString(detail);
//				out.write(jsonStr);
//				out.write(System.lineSeparator());
				data.syncSendMessage("device-data", key, jsonStr);
				key++;
				Thread.sleep(100);
			}
			
			
//			// generate 2 data after 12:00
//			time = format.parse("2018-04-17 12:20:00");
//			gc.setTime(time);
//			for (int i = 0; i < 2; i++) {
//				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01",101,gc.getTime().getTime());
//				gc.add(Calendar.MINUTE, 30);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				String jsonStr = mapper.writeValueAsString(detail);
//				data.syncSendMessage("device-data", key, jsonStr);
////				out.write(jsonStr);
////				out.write(System.lineSeparator());
//				key++;
//			}
//			
//			// generate 2 data after 18:00
//			time = format.parse("2018-04-17 18:20:00");
//			gc.setTime(time);
//			for (int i = 0; i < 2; i++) {
//				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01",101,gc.getTime().getTime());
//				gc.add(Calendar.MINUTE, 30);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				String jsonStr = mapper.writeValueAsString(detail);
//				data.syncSendMessage("device-data", key, jsonStr);
//				key++;
//			}
//			// generate 2 data befor 12:00
//			//sleep 1 minutes. and generate 2 data before 12:00
//			Thread.sleep(60*1000*10);
//			
//			time = format.parse("2018-04-17 09:00:00");
//			gc.setTime(time);
//			for (int i = 0; i < 2; i++) {
//				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01",101,gc.getTime().getTime());
//				gc.add(Calendar.MINUTE, 30);
//				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
//				String jsonStr = mapper.writeValueAsString(detail);
//				data.syncSendMessage("device-data", key, jsonStr);
//				key++;
//			}
//			out.flush();
//			out.close();
			
		} catch (IOException | ParseException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	public static void prepareOrderData() {
		
		try {
			FileWriter out = new FileWriter("deviceInput.txt");
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date time = format.parse("2018-04-17 08:05:00");
			Calendar gc = Calendar.getInstance();
			gc.setTime(time);

			// SimpleDateFormat
			// generate 100 device =1 and app 101
			for (int i = 0; i < 2; i++) {
				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01", 101,gc.getTime().getTime());
				gc.add(Calendar.MINUTE, 30);
				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 101);
				String jsonStr = mapper.writeValueAsString(detail);
				out.write(jsonStr);
				out.write(System.lineSeparator());
			}
			time = format.parse("2018-04-17 12:20:00");
			gc.setTime(time);
			// generate 100 device =1 and app 102
			for (int i = 0; i < 2; i++) {
				DeviceDetail detail = new  DeviceDetail(1,"B4:82:45:00:00:01", 101,gc.getTime().getTime());
				gc.add(Calendar.MINUTE, 30);
				//DeviceDetail detail = new DeviceDetail(1, "B4:82:45:00:00:01", 102);
				String jsonStr = mapper.writeValueAsString(detail);
				out.write(jsonStr);
				out.write(System.lineSeparator());
			}

			out.flush();
			out.close();
			
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}
	
	public void syncSendMessage(String topic, Integer key, String msg) {

		ProducerRecord<Object, String> record = new ProducerRecord<Object, String>(topic, key, msg);
		try {
			RecordMetadata metaData = producer.send(record).get();
			LOG.info("message key:{}, msg:{}. send to partition :{}", key, msg, metaData.partition());
			LOG.info("timestamp of message is {}, the offset is :{}", metaData.timestamp(), metaData.offset());
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("send message failed!", e);
		}
	}

}
