package com.examples.kafka.message.consumer;

import java.util.Arrays;
import java.util.List;

public class LogReceive extends BaseReceive {

	public LogReceive(String name, String group, List<String> topics) {
		super(name, group, topics);
	}
	
	public static void main(String[] args) {
		String name = "log-consumer";
		String group = "log";
		List<String> topics = Arrays.asList(new String[] { "sport",  "finance",  "ent",   "sport-photo", "ent-photo", "finance-photo" });
		PhotoNewsReceive receive = new PhotoNewsReceive(name, group, topics);
		receive.subsribe();
		receive.receiveMsg();
	}
}
