package com.examples.kafka.message.consumer;

import java.util.Arrays;
import java.util.List;

public class PhotoNewsReceive extends BaseReceive {
	
//	private final static Logger LOG = LoggerFactory.getLogger(PhotoNewsReceive.class);
//	private final AtomicBoolean closed = new AtomicBoolean(false);
//	private String name;
	//private final KafkaConsumer<String, String> consumer;
	public PhotoNewsReceive(String name, String group, List<String> topics) {
		super(name, group, topics);
	}
	
	public static void main(String[] args) {
		String name = "sport-consumer";
		String group = "photos";
		List<String> topics = Arrays.asList(new String[] { "sport-photo", "ent-photo", "finance-photo" });
		PhotoNewsReceive receive = new PhotoNewsReceive(name, group, topics);
		receive.subsribe();
		receive.receiveMsg();
	}
	
}
