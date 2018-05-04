package com.examples.kafka.message.consumer;

import java.util.Arrays;
import java.util.List;

public class FinanceNewsReceive extends BaseReceive {

	public FinanceNewsReceive(String name, String group, List<String> topics) {
		super(name, group, topics);
	}
	
	public static void main(String[] args) {
		String name = "finance-consumer";
		String group = "finance";
		List<String> topics = Arrays.asList(new String[] { "finance-photo", "finance"});
		PhotoNewsReceive receive = new PhotoNewsReceive(name, group, topics);
		receive.subsribe();
		receive.receiveMsg();
	}

}
