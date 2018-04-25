package com.examples.flink.stream;

import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.examples.flink.model.DeviceAggr;
import com.examples.flink.model.DeviceDetail;
import com.examples.flink.stream.DeviceJob.DeviceTimeStamp;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProcessWindowTimeJob {

	private static ObjectMapper mapper = new ObjectMapper();

	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static Logger LOG = LoggerFactory.getLogger(ProcessWindowTimeJob.class);

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.readTextFile("deviceInputLateness.txt");
		// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<DeviceDetail> detailDs = text.flatMap((value, out) -> {
			DeviceDetail device = mapper.readValue(value, DeviceDetail.class);
			out.collect(device);
		});

		AggregateFunction<DeviceDetail, Tuple4<Integer, Integer, Long, Long>, DeviceAggr> aggreFun = new AggregateFunction<DeviceDetail, Tuple4<Integer, Integer, Long, Long>, DeviceAggr>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple4<Integer, Integer, Long, Long> createAccumulator() {
				LOG.info("create accumulater<info>");
				return new Tuple4<Integer, Integer, Long, Long>(0, 0, 0L, 0L);
			}

			
			@Override
			public Tuple4<Integer, Integer, Long, Long> add(DeviceDetail value,
					Tuple4<Integer, Integer, Long, Long> accumulator) {
					accumulator.f0 = value.getAppId();
					accumulator.f1 = value.getDeviceId();
					accumulator.f2 = value.getUpBytes() + accumulator.f2;
					accumulator.f3 = value.getDownBytes() + accumulator.f3;
					LOG.info("accumulator add<info>, the add value is appId -> {}, event time is {}", value.getAppId(),
					value.getEventTime());
					return accumulator;
			}
			

			@Override
			public DeviceAggr getResult(Tuple4<Integer, Integer, Long, Long> accumulator) {
				DeviceAggr result = new DeviceAggr();
				result.setAppId(accumulator.f0);
				result.setDeviceId(accumulator.f1);
				result.setUpBytes(accumulator.f2);
				result.setDownBytes(accumulator.f3);
				LOG.info("accumulator getResult<info>, the result value is appId -> {}, the up value  is {}",
						accumulator.f0, accumulator.f2);
				return result;
			}

			@Override
			public Tuple4<Integer, Integer, Long, Long> merge(Tuple4<Integer, Integer, Long, Long> a,
					Tuple4<Integer, Integer, Long, Long> b) {
				LOG.info("accumulator merge<info>");
				Tuple4<Integer, Integer, Long, Long> accumulator = new Tuple4<Integer, Integer, Long, Long>(0, 0, 0L,
						0L);
				accumulator.f0 = a.f0;
				accumulator.f1 = a.f1;
				accumulator.f2 = a.f2 + b.f2;
				accumulator.f3 = a.f3 + b.f3;
				return accumulator;
			}
		};
		ProcessWindowFunction<DeviceAggr, DeviceAggr, Tuple, TimeWindow> processWindowFunction = new ProcessWindowFunction<DeviceAggr, DeviceAggr, Tuple, TimeWindow>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void process(Tuple key, Context context, Iterable<DeviceAggr> result,
					Collector<DeviceAggr> out) throws Exception {
				DeviceAggr tmp = result.iterator().next();
				Long aggTs = context.window().getStart();
				tmp.setAggTs(aggTs);
				String aggTime = format.format(aggTs);
				tmp.setAggTime(aggTime);
				out.collect(tmp);
			}

		};
		DataStream<DeviceAggr> aggrDs = detailDs.assignTimestampsAndWatermarks(new DeviceTimeStamp())
				.name("water-device").keyBy("deviceId", "appId")
				.window(new CustomerTumblingEventTimeWindows(Time.hours(6).toMilliseconds(), 0L))
				.aggregate(aggreFun, processWindowFunction)
				.name("device-aggreate-stream");
		
		aggrDs.writeAsText("devicOutputeWindowTime.txt",WriteMode.OVERWRITE).name("fileSink").setParallelism(1);
        //aggrDs.print().name("console-sink");
        //windowCounts.writeAsText(path)
        //aggrDs.print().name("consoleSink").setParallelism(1);
        env.execute("device text");

	}

}
