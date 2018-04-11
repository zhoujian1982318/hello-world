package com.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.examples.service.HelloMessageService;
import com.examples.service.TestService;

//@SpringBootApplication //等价于使用@Configuration, @EnableAutoConfiguration 和 @ComponentScan //似乎不用  @Configuration @Bean testService 也可以注册
@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages="com.examples.service")
public class ConsoleApplication implements CommandLineRunner {
	
	private static Logger LOG = LoggerFactory.getLogger(ConsoleApplication.class);
	
	@Autowired
	private HelloMessageService helloService;
	
	@Autowired
	private TestService testService;
	
	
	public static void main(String[] args) {
		ApplicationContext ctx  = SpringApplication.run(ConsoleApplication.class, args);
		LOG.info("the ctx name is {}", ctx.getClass().getName());
	}
	
	
	@Override
	public void run(String... args) throws Exception {
		
		  if (args.length > 0) {
			  LOG.info(helloService.getMessage(args[0].toString()));
	       } else {
	          LOG.info(helloService.getMessage());
	       }
		   LOG.info("the testService get Message is {}", testService.getMessage());
	}
	
	
	
	@Bean
	public TestService testService() {
		return new TestService();
	}

}
