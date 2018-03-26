package com.example.dubbo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.examples.entity.Ap;
import com.examples.service.ApService;

public class Consumer {
	
	private static final  Logger LOG = LoggerFactory.getLogger(Consumer.class);
	public static void main(String[] args) {
		 System.setProperty("java.net.preferIPv4Stack", "true");
		 ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"META-INF/spring/hello-dubbo-consumer.xml"});
		 context.start();
		 
		 ApService apService = (ApService) context.getBean("apService"); // get remote service proxy
		 
		for (int i = 0; i < 2; i++) {
			 Ap ap = new Ap();
			 //ap.setApMac("00:00:00:00:00:01");
			 ap.setApName("test");
			 int response =  apService.addAp(ap);
			 LOG.info("invode remote method succeffully! ,the response is {}", response);
		}
		
		for (int i = 0; i < 2; i++) {
			 Ap ap = new Ap();
			 ap.setApMac("00:00:00:00:00:01");
			 ap.setApName("test");
			 int response =  apService.addAp(ap);
			 LOG.info("invode remote method succeffully! ,the response is {}", response);
		}
		
		for (int i = 0; i < 2; i++) {
			 Ap ap = new Ap();
			 ap.setApMac("00:00:00:00:00:01");
			 ap.setApName("error");
			 int response =  apService.addAp(ap);
			 LOG.info("invode remote method succeffully! ,the response is {}", response);
		}
		
	}
}
