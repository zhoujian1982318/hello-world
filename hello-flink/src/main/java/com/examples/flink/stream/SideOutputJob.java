package com.examples.flink.stream;

import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.DeviceAggr;
import com.examples.flink.model.DeviceDetail;
import com.examples.flink.stream.DeviceJob.DeviceTimeStamp;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SideOutputJob {
	
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static Logger LOG = LoggerFactory.getLogger(SideOutputJob.class);
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.22.0.81:9092");
		// only required for Kafka 0.8
		//properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> latenessDs = env
					.addSource(new FlinkKafkaConsumer010<>("device-data", new SimpleStringSchema(), properties));
		
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
     	@SuppressWarnings("serial")
		final OutputTag<DeviceDetail> lateOutputTag = new OutputTag<DeviceDetail>("late-data"){};
     	
		SingleOutputStreamOperator<DeviceAggr> aggrDs = detailDs
				.assignTimestampsAndWatermarks(new DeviceTimeStamp())
				.name("water-device")
				.keyBy("deviceId", "appId")
				//.window(TumblingEventTimeWindows.of(Time.hours(6)))，自定义窗口的开始时间和结束时间。 
				//要不让系统会根据element event time算出 开始时间和结束时间 TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
				.window(new CustomerTumblingEventTimeWindows(Time.hours(6).toMilliseconds(), 0L))
				.allowedLateness(Time.seconds(10))
				.sideOutputLateData(lateOutputTag)
				//.allowedLateness(Time.hours(12))
			   .aggregate(aggreFun).name("device-aggreate-stream");
     	
     	DataStream<DeviceDetail> lateStream = aggrDs.getSideOutput(lateOutputTag);
     	
     	lateStream.writeAsText("sideOutput.txt",WriteMode.OVERWRITE).name("fileSideOutputSink").setParallelism(1);
     	
     	aggrDs.writeAsText("output.txt",WriteMode.OVERWRITE).name("fileSideOutputSink").setParallelism(1);
       //aggrDs.print().name("console-sink");
       //windowCounts.writeAsText(path)
       //aggrDs.print().name("consoleSink").setParallelism(1);
       env.execute("device sdie out put text");
	}
}
