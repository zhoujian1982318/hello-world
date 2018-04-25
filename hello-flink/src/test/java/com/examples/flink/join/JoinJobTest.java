package com.examples.flink.join;

import org.apache.flink.streaming.util.StreamingProgramTestBase;

public class JoinJobTest extends StreamingProgramTestBase {
	
	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1); 
	}
	@Override
	protected void testProgram() throws Exception {
		JoinJob.main(new String[] {});
	}

}
