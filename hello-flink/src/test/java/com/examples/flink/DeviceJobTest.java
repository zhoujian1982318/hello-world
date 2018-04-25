package com.examples.flink;

import org.apache.flink.streaming.util.StreamingProgramTestBase;

import com.examples.flink.stream.DeviceJob;

public class DeviceJobTest extends StreamingProgramTestBase {
	
	
	
	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1); 
	}

	@Override
	protected void testProgram() throws Exception {
		DeviceJob.main(new String[]{});

	}
	
	

}
