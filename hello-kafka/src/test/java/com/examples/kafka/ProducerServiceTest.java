package com.examples.kafka;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
@RunWith(BlockJUnit4ClassRunner.class)
public class ProducerServiceTest {
	
	public static ProducerService producerService;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		producerService = new ProducerService();
	}
	
	@Test
	public void syncSendMessage() {
//		producerService.syncSendMessage("test", 7, "message_07");
//		producerService.syncSendMessage("test", 8, "message_08");
//		producerService.syncSendMessage("test", 9, "message_09");
//		producerService.syncSendMessage("test", 10, "message_10");
//		producerService.syncSendMessage("test", 11, "message_11");
		producerService.syncSendMessage("test", 1, "message_01");
		producerService.syncSendMessage("test", 2, "message_02");
		producerService.syncSendMessage("test", 3, "message_03");
		producerService.syncSendMessage("test", 4, "message_04");
		producerService.syncSendMessage("test", 5, "message_05");
		producerService.syncSendMessage("test", 6, "message_06");
	}
	
//	@Test
//	public void asyncSendMessage() {
//		producerService.asyncSendMessage("test", 2, "message_02");
//	}
//	
//	@Test
//	public void getMetrics() {
//		producerService.getMetrics();
//	}

}
