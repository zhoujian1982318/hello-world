package com.examples.flink.join;



import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.BookOrder;
import com.examples.flink.model.PhoneOrder;

public class JoinJob {
	
	private static Logger LOG = LoggerFactory.getLogger(JoinJob.class);
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
	       
        DataStream<BookOrder> bookOrdesDs = env.readTextFile("BookOrderInput.txt").map((value)->{
        	String[] strs = value.split(",");
        	BookOrder bookOrder = new BookOrder();
        	bookOrder.setUserName(strs[0]);
        	bookOrder.setBookName(strs[1]);
        	bookOrder.setPrice(Integer.parseInt(strs[2]));
        	bookOrder.setCount(Integer.parseInt(strs[3]));
        	return bookOrder;
        });
        
        DataStream<PhoneOrder> phoneOrderDs = env.readTextFile("PhoneOrderInput.txt").map((value)->{
        	String[] strs = value.split(",");
        	PhoneOrder phoneOrder = new PhoneOrder();
        	phoneOrder.setUserName(strs[0]);
        	phoneOrder.setPhoneName(strs[1]);
        	phoneOrder.setPrice(Integer.parseInt(strs[2]));
        	phoneOrder.setCount(Integer.parseInt(strs[3]));
        	return phoneOrder;
        });
        
//       DataStream<Tuple5<String,String,String, Integer, Integer>> bookPhoneDs = bookOrdesDs.join(phoneOrderDs).where((value)->{
//        	String userName = value.getUserName();
//        	return userName;
//        }).equalTo((param)->{
//        	String userName = param.getUserName();
//        	return userName;
//        }).window(TumblingEventTimeWindows.of(Time.seconds(60)))
//        .apply((BookOrder bookOrder, PhoneOrder phoneOrder)->{
//        	String userName = bookOrder.getUserName();
//        	String bookName = bookOrder.getBookName();
//        	String phoneName = phoneOrder.getPhoneName();
//        	Integer bookPrice = bookOrder.getPrice();
//        	Integer phonePrice = phoneOrder.getPrice();
////        	Integer bookCount = bookOrder.getCount();
////        	Integer phoneCount = phoneOrder.getCount();
//        	//只能得到笛卡尔集合， 不能过滤了。如果想得到 count 相等的 book 和 phone. 要用 CoGroupedStreams 
////        	if(bookCount==phoneCount) {
//        		Tuple5<String,String,String, Integer, Integer> bookAndPhone = new Tuple5<>(userName,bookName,phoneName,bookPrice,phonePrice);
//        		return bookAndPhone;
////        	}else {
////        		return null;
////        	}
//        });
       
       
       DataStream<Tuple5<String,String,String, Integer, Integer>> bookPhoneDs = bookOrdesDs.coGroup(phoneOrderDs).where((value)->{
        	String userName = value.getUserName();
        	return userName;
        }).equalTo((param)->{
        	String userName = param.getUserName();
        	return userName;
        //Register an event-time callback. When the current watermark passes the specified
        //time {@link Trigger#onEventTime(long, Window, TriggerContext)} is called with the time specified here.
        //ctx.registerEventTimeTimer(window.maxTimestamp());
        //如果定义了watermark， 似乎跟当前时间没有关系， 只跟 element 发送上来的 Event time 关联，当发上来的 event time 超过 windown endtime. 就会触发
        //如果是 IngestionTime。好像都会触发 ??, 可能跟我适用的 source 有关， 如果用Kafka source 就不会， 可能是触发了任务结束，也会触发这个trigger
        //Custom File Source -> Map -> Map (1/1) (93f0b8edd2b0cf0b0d038708b698524e) switched from RUNNING to FINISHED.
        }).window(TumblingEventTimeWindows.of(Time.hours(1)))
        .apply((Iterable<BookOrder> first,  Iterable<PhoneOrder> second, Collector<Tuple5<String,String,String, Integer, Integer>> out)->{
        	LOG.info("computer in the operator...");
        	for (BookOrder bookOrder : first) {
        		for(PhoneOrder phoneOrder: second) {
        			if(phoneOrder.getCount() == bookOrder.getCount()) {
        				String userName = bookOrder.getUserName();
        	        	String bookName = bookOrder.getBookName();
        	        	String phoneName = phoneOrder.getPhoneName();
        	            Integer bookPrice = bookOrder.getPrice();
        	        	Integer phonePrice = phoneOrder.getPrice();
        				Tuple5<String,String,String, Integer, Integer> bookAndPhone = new Tuple5<>(userName,bookName,phoneName,bookPrice,phonePrice);
        				out.collect(bookAndPhone);
        			}
        		}
			}
        });
       
       bookPhoneDs.writeAsText("joinJobOutput.txt",WriteMode.OVERWRITE).name("fileSink").setParallelism(1);
       
       env.execute("execute join job");
       
       System.in.read();
	}
	
}
