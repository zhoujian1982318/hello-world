package com.examples.kafka.message.consumer;

import java.util.Arrays;
import java.util.List;

public class SportNewsReceive extends BaseReceive {
	

	public SportNewsReceive(String name, String group, List<String> topics) {
		super(name, group, topics);
	}

	public static void main(String[] args) throws Exception {
		String name = "sport-consumer-1";
		String group = "sports";
		List<String> topics = Arrays.asList(new String[] { "sport", "sport-photo"});
		SportNewsReceive receive = new SportNewsReceive(name,group, topics);
		receive.subsribe();
		receive.receiveMsg();
	}
}
