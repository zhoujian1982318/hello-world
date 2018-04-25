package com.examples.flink.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;

public abstract class CheckPointJobTest extends AbstractTestBase {

	public CheckPointJobTest() {
		super(new Configuration());
		//Configuration config = new Configuration();
	}
	
//	public StreamingProgramTestBase() {
//		super(new Configuration());
//		setParallelism(DEFAULT_PARALLELISM);
//	}
	
//	public CheckPointJobTest() {
//		Configuration configuration =  new Configuration();
//		//configuration.setString(key, value);
//		super(configuration);
//	}

}
