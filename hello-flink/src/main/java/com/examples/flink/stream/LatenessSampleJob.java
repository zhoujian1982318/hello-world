package com.examples.flink.stream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.DeviceAggr;
import com.examples.flink.model.DeviceDetail;
import com.examples.flink.stream.DeviceJob.DeviceTimeStamp;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LatenessSampleJob {
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static Logger LOG = LoggerFactory.getLogger(LatenessSampleJob.class);
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.22.0.81:9092");
		// only required for Kafka 0.8
		//properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> latenessDs = env
					.addSource(new FlinkKafkaConsumer010<>("device-data", new SimpleStringSchema(), properties));


        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
       
        
        //env.from
        
        DataStream<DeviceDetail> detailDs = latenessDs.flatMap((String value, Collector<DeviceDetail> out)-> {
        	DeviceDetail  device = mapper.readValue(value, DeviceDetail.class);
        	out.collect(device);
        });
        
       
        AggregateFunction<DeviceDetail, DeviceAggr, DeviceAggr> aggreFun = new AggregateFunction<DeviceDetail, DeviceAggr, DeviceAggr>() {
        		/**
     			 * 
     			 */
     			private static final long serialVersionUID = 1L;

     			@Override
     			public DeviceAggr createAccumulator() {
     				LOG.info("create accumulater<info>");
     				return new DeviceAggr();
     			}

     			@Override
     			public DeviceAggr add(DeviceDetail value, DeviceAggr accumulator) {
     				accumulator.setAppId(value.getAppId());
     				accumulator.setDeviceId(value.getDeviceId());
     				accumulator.setDownBytes(value.getDownBytes()+accumulator.getDownBytes());
     				accumulator.setUpBytes(value.getUpBytes()+accumulator.getUpBytes());
     				LOG.info("accumulator add<info>, the add value is appId -> {}, event time is {}", value.getAppId(), value.getEventTime());
     				return accumulator;
     			}

     			@Override
     			public DeviceAggr getResult(DeviceAggr accumulator) {
     				LOG.info("accumulator getResult<info>, the result value is appId -> {}, the up value  is {}", accumulator.getAppId(), accumulator.getUpBytes());
     				return accumulator;
     			}

     			@Override
     			public DeviceAggr merge(DeviceAggr a, DeviceAggr b) {
     				LOG.info("accumulator merge<info>");
     				DeviceAggr accumulator = new DeviceAggr();
     				accumulator.setDeviceId(a.getDeviceId());
     				accumulator.setAppId(a.getAppId());
     				accumulator.setDownBytes(a.getDownBytes()+b.getDownBytes());
     				accumulator.setUpBytes(a.getUpBytes()+b.getUpBytes());
     				return accumulator;
     			}
     	}; 
     	
     	DataStream<DeviceAggr> aggrDs = detailDs
				//.assignTimestampsAndWatermarks(new LatenessTimeStamp(Time.minutes(2)))
//				.assignTimestampsAndWatermarks(new DeviceTimeStamp())
//				.name("water-device")
				.keyBy("deviceId", "appId")
				//.window(TumblingEventTimeWindows.of(Time.hours(6)))，自定义窗口的开始时间和结束时间。 
				//要不让系统会根据element event time算出 开始时间和结束时间 TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
//				.window(new CustomerTumblingEventTimeWindows(Time.hours(6).toMilliseconds(), 0L))
				////如果是 IngestionTime。即使没有 element 发送上来的  如果过来当前时间， 也会触发，  Trigger#onEventTime(long, Window, TriggerContext)} 
				.timeWindow(Time.hours(1))
				//allowedLateness 不需要与 BoundedOutOfOrdernessTimestampExtractor 结合， 只是时间跟 event time 有关，跟elements到达的时间没有关系
				//这个allowedLateness 与 event time 相关，
				.allowedLateness(Time.hours(12))
				
				.aggregate(aggreFun).name("device-aggreate-stream");
     	
       aggrDs.writeAsText("deviceOnputLateness.txt",WriteMode.OVERWRITE).name("fileSink").setParallelism(1);
       //aggrDs.print().name("console-sink");
       //windowCounts.writeAsText(path)
       //aggrDs.print().name("consoleSink").setParallelism(1);
       env.execute("device lateness text");
	}
	
	
	
	public static class LatenessTimeStamp extends BoundedOutOfOrdernessTimestampExtractor<DeviceDetail>{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		private Logger logger = LoggerFactory.getLogger(LatenessTimeStamp.class);

		public LatenessTimeStamp(Time maxOutOfOrderness) {
			super(maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(DeviceDetail element) {
			long current = element.getEventTs() == null ? 0 : element.getEventTs();
			long watermark = getCurrentWatermark().getTimestamp();
			
			logger.info("event timestamp: {}, {}, watermark: {}, {}", current, sdf.format(new Date(current)), watermark, sdf.format(new Date(watermark)));
			return current;
		}

		/**
		 * 
		 */
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public long extractAscendingTimestamp(DeviceDetail element) {
//			// TODO Auto-generated method stub
//			return element.getEventTs();
//		}
		
	}
}
