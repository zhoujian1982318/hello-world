package com.examples.flink.state;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
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
import com.examples.flink.state.ManagedKeyedStateJob.UpBytesWindowAverage;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CheckPointJob {
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static Logger LOG = LoggerFactory.getLogger(CheckPointJob.class);
	
	public static void main(String[] args) throws Exception {
		
//		Configuration config = new Configuration();
//		config.setString(CoreOptions.CHECKPOINTS_DIRECTORY, "file:///d:/temp/meta/flink-checkpoint");
//		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
//		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
//
//		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false);
		
//		LocalFlinkMiniCluster cluster = TestBaseUtils.startCluster(config, true);
//		TestStreamEnvironment.setAsContext(cluster, 1);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//set state backend to file system. save the state date to checkpoint (not meta data )
		env.setStateBackend(new FsStateBackend("file:///home/r2/bigdata/flink/check-point/"));
		//如果 enable external  checkpoint , 必须要设置 state.checkpoints.dir
		//如果设置成 DELETE_ON_CANCELLATION。  state data 和 meta data，将会被删除。 当取消job. 效果和 没有 enable external 差不多
		//所以要设置成  RETAIN_ON_CANCELLATION
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.enableCheckpointing(20000);
		
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		
		LOG.info("<info> the checkpoint config {}", checkpointConfig.getExternalizedCheckpointCleanup());
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.22.0.81:9092");
		// only required for Kafka 0.8
		//properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> kafkaDs = env
					.addSource(new FlinkKafkaConsumer010<>("device-data", new SimpleStringSchema(), properties)).uid("source-kafka-0001");
		
		DataStream<DeviceDetail> detailDs = kafkaDs.flatMap((String value, Collector<DeviceDetail> out)-> {
	        	DeviceDetail  device = mapper.readValue(value, DeviceDetail.class);
	        	out.collect(device);
	    }).uid("flap-map-device-detail-0002");
		
		DataStream<Tuple2<String, Long>> upBytesDs =  detailDs.flatMap((DeviceDetail value, Collector<Tuple2<String, Long>> out)->{
			String deviceId = String.valueOf(value.getDeviceId());
			Long upBytes = value.getUpBytes();
			out.collect(new Tuple2<>(deviceId, upBytes));
		}).uid("flap-map-tuple-0003");
		
		DataStream<Tuple2<String, Long>> aggrDs = upBytesDs.keyBy(0).flatMap(new UpBytesWindowAverage()).uid("flap-map-keyby-deviceId-sum-0003");
		

		
	    aggrDs.writeAsText("checkpoint.txt",WriteMode.OVERWRITE).name("fileSink").uid("file-sink-0004");
	    
	    env.execute("check point job");
	}
}
