package com.examples.flink.stream;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CustomerTumblingEventTimeWindows extends TumblingEventTimeWindows {
	
	private final long size;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected CustomerTumblingEventTimeWindows(long size, long offset) {
		super(size, offset);
		this.size = size;
	}

	// 00:00~6:00
	// 06:00~12:00
	// 12:00~18:00
	// 18:00~24:00
	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		if (timestamp > Long.MIN_VALUE) {
			  Calendar c = Calendar.getInstance();
			  c.setTimeInMillis(timestamp);
			  int hour = c.get(Calendar.HOUR);
			  c.set(Calendar.MINUTE, 0);
			  c.set(Calendar.SECOND, 0);
			  c.set(Calendar.MILLISECOND, 0);
			  if(hour>=0 && hour< 6) {
				  c.set(Calendar.HOUR, 0);
			  }
			  if((hour>=6 && hour<12)) {
				  c.set(Calendar.HOUR, 6);
			  }
			  if((hour>=12 && hour<18)) {
				  c.set(Calendar.HOUR, 12);
			  }
			  if((hour>=18 && hour<24)) {
				  c.set(Calendar.HOUR, 18);
			  }
			  long start = c.getTimeInMillis();
			  //timestamp
			  //long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
			  return Collections.singletonList(new TimeWindow(start, start + size));
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
					"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
					"'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}
	
	

}
