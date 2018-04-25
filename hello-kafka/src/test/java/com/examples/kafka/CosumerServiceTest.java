package com.examples.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

public class CosumerServiceTest {

	@Test
	public void testReceiveMsgFromBegin() {
		Map<String,String> configPros = new HashMap<String,String>();
		//earliest
		//当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
		//latest
		//当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
		configPros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerService service = new ConsumerService("group1", "name1", "test", configPros);
		
		//enable.auto.commit = true;
		//auto.commit.interval.ms = 5000, 5 秒钟会自动提交
		///enable.auto.commit = false, 不自动提交。消费者自己控制
		Thread t =new Thread(service);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
