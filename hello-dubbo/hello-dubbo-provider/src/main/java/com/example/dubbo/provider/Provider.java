package com.example.dubbo.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Provider {
	private static final  Logger LOG = LoggerFactory.getLogger(Provider.class);
	
	public static void main(String[] args) throws Exception {
		System.setProperty("java.net.preferIPv4Stack", "true");
		LOG.info("load spring context.");
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"META-INF/spring/hello-dubbo-provider.xml"});
        context.start();
    	synchronized (Provider.class) {
			while (true) {
				try {
					Provider.class.wait();
				} catch (InterruptedException e) {
					LOG.warn("the current thread is interrupted", e);
				}
			}
		}
    	
	}
}
