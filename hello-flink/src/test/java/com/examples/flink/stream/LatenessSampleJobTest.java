package com.examples.flink.stream;

import org.apache.flink.streaming.util.StreamingProgramTestBase;

public class LatenessSampleJobTest extends StreamingProgramTestBase{
	
	
	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1); 
	}
	
	@Override
	protected void testProgram() throws Exception {
		LatenessSampleJob.main(new String[]{});
		
	}

}
