package com.examples.flink.state;

import java.util.Properties;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.DeviceDetail;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ManagedKeyedStateJob {
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static Logger LOG = LoggerFactory.getLogger(ManagedKeyedStateJob.class);
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//set state backend to file system. save the state date to checkpoint (not meta data )
		env.setStateBackend(new FsStateBackend("file:///d:/temp/flink-checkpoint"));
		//如果 enable external  checkpoint , 必须要设置 state.checkpoints.dir
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.enableCheckpointing(5000);
		CheckpointConfig config = env.getCheckpointConfig();
		LOG.info("<info> the checkpoint config {}",config);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.22.0.81:9092");
		// only required for Kafka 0.8
		//properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> kafkaDs = env
					.addSource(new FlinkKafkaConsumer010<>("device-data", new SimpleStringSchema(), properties));
		
		DataStream<DeviceDetail> detailDs = kafkaDs.flatMap((String value, Collector<DeviceDetail> out)-> {
	        	DeviceDetail  device = mapper.readValue(value, DeviceDetail.class);
	        	out.collect(device);
	    });
		
		DataStream<Tuple2<String, Long>> upBytesDs =  detailDs.flatMap((DeviceDetail value, Collector<Tuple2<String, Long>> out)->{
			String deviceId = String.valueOf(value.getDeviceId());
			Long upBytes = value.getUpBytes();
			out.collect(new Tuple2<>(deviceId, upBytes));
		});
		
		DataStream<Tuple2<String, Long>> aggrDs = upBytesDs.keyBy(0).flatMap(new UpBytesWindowAverage());
		
//		DataStream<String> text = env.readTextFile("keystate.txt");
		
//		DataStream<Tuple2<String, Long>> aggrDs = text.flatMap((String value, Collector<DeviceDetail> out)-> {
//		    	DeviceDetail  device = mapper.readValue(value, DeviceDetail.class);
//		    	out.collect(device);
//		})
//		DataStream<Tuple2<String, Long>> aggrDs = text.flatMap((String value, Collector<Tuple2<String,Long>> out)->{
//			 String[] strs = value.split(",");
//			 out.collect(new Tuple2<>(strs[0],Long.valueOf(strs[1])));
//		}).keyBy(0)
//	        .flatMap(new UpBytesWindowAverage());
		
	    aggrDs.writeAsText("keystateOutput.txt",WriteMode.OVERWRITE).name("fileSink").setParallelism(1);
	    
	    env.execute("keyed state job");
	     
	}
	
	
	
	public static class UpBytesWindowAverage extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		/**
	     * The ValueState handle. The first field is the count, the second field a running sum upbytes.
	     */
	    private transient ValueState<Tuple2<Integer, Long>> sum;
	    
		@Override
		public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
			Tuple2<Integer, Long> currentSum = sum.value();
			
			currentSum.f0 +=1;
			currentSum.f1 += value.f1;
			
			sum.update(currentSum);
			LOG.info("state<info> the input value is {}",value);
			LOG.info("state<info> the value state is {}, the class name is {}",sum, sum.getClass().getName());
			if (currentSum.f0 >= 10) {
				LOG.info("state<info> the current value is {}",currentSum);
	            out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
	            sum.clear();
	        }
			
		}
		@Override
		public void open(Configuration parameters) throws Exception {
			 ValueStateDescriptor<Tuple2<Integer, Long>> descriptor =
		                new ValueStateDescriptor<>(
		                        "average", // the state name
		                        TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {})
		                        ,Tuple2.of(0, 0L)); // default value of the state, if nothing was set
		     sum = getRuntimeContext().getState(descriptor);
		}
		
		
		
	}
}
