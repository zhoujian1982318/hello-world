package com.examples.kafka.message.consumer;

import java.util.Arrays;
import java.util.List;

public class EntNewsReceive extends BaseReceive {
	
	public EntNewsReceive(String name, String group, List<String> topics) {
		super(name,group, topics);
	}
	
	public static void main(String[] args) {
		String name = "ent-consumer";
		String group = "entertainment";
		List<String> topics = Arrays.asList(new String[] { "ent-photo", "ent" });
		PhotoNewsReceive receive = new PhotoNewsReceive(name, group, topics);
		receive.subsribe();
		receive.receiveMsg();
	}
}
