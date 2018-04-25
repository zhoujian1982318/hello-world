package com.examples.flink.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.DeviceAggr;
import com.examples.flink.model.DeviceDetail;
import com.fasterxml.jackson.databind.ObjectMapper;



public class DeviceJob {
	
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static Logger LOG = LoggerFactory.getLogger(DeviceJob.class);
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       
        DataStream<String> text = env.readTextFile("deviceInput.txt");
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // parse the data, group it, window it, and aggregate the counts
        DataStream<DeviceDetail> detailDs =  text.flatMap(new FlatMapFunction<String, DeviceDetail>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String value, Collector<DeviceDetail> out) throws Exception {
				DeviceDetail  tmp = mapper.readValue(value, DeviceDetail.class);
				out.collect(tmp);
				
			}
		});
//        DataStream<DeviceDetail> aggrDs =  
//        			detailDs.keyBy("deviceId", "appId").reduce(new ReduceFunction<DeviceDetail>() {
//					private static final long serialVersionUID = 1L;
//					@Override
//					public DeviceDetail reduce(DeviceDetail value1, DeviceDetail value2) throws Exception {
//						DeviceDetail device = new DeviceDetail();
//						device.setDeviceId(value1.getDeviceId());
//						device.setAppId(value1.getAppId());
//						device.setDownBytes(value1.getDownBytes()+value2.getDownBytes());
//						device.setUpBytes(value1.getUpBytes()+value2.getUpBytes());
//						return device;
//					}
//		}).name("device-aggrate");
        
//		DataStream<DeviceDetail> aggrDs =  
//				detailDs.keyBy("deviceId", "appId").fold(new DeviceDetail(), new FoldFunction<DeviceDetail, DeviceDetail>(){
//
//					@Override
//					public DeviceDetail fold(DeviceDetail accumulator, DeviceDetail value) throws Exception {
//						if(null == accumulator.getDeviceId()){
//							accumulator.setAppId(value.getAppId());
//							accumulator.setDeviceId(value.getDeviceId());
//							accumulator.setAppName(value.getAppName());
//						}
//						accumulator.setUpBytes(accumulator.getUpBytes() + value.getUpBytes());
//						accumulator.setDownBytes(accumulator.getDownBytes() + value.getDownBytes());
//						return accumulator;
//					}
//					
//				});
        
        
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
        							.assignTimestampsAndWatermarks(new DeviceTimeStamp()).name("water-device")
        							.keyBy("deviceId", "appId")
        							//.window(TumblingEventTimeWindows.of(Time.hours(6)))
        							.window(new CustomerTumblingEventTimeWindows(Time.hours(6).toMilliseconds(), 0L))
        							//.allowedLateness(Time.hours(7))
        							//.timeWindow(Time.hours(7))
        							.aggregate(aggreFun).name("device-aggreate-stream");
        
        		
        aggrDs.writeAsText("deviceOnput.txt",WriteMode.OVERWRITE).name("fileSink").setParallelism(1);
        //aggrDs.print().name("console-sink");
        //windowCounts.writeAsText(path)
        //aggrDs.print().name("consoleSink").setParallelism(1);
        env.execute("device text");
        
       
	}
	
	public static class DeviceTimeStamp extends AscendingTimestampExtractor<DeviceDetail>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(DeviceDetail element) {
			// TODO Auto-generated method stub
			return element.getEventTs();
		}
		
	}
}
