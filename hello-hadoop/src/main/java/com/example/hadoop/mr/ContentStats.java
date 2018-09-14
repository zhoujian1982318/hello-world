package com.example.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ContentStats implements Writable {
	
	private int hits;
	
	private long size;
	
	private char seperator = ',';
	
	public ContentStats() {}
	
	public ContentStats(int hits, long size) {
		this.hits = hits;
		this.size = size;
	}
	
	
	

	public int getHits() {
		return hits;
	}




	public void setHits(int hits) {
		this.hits = hits;
	}




	public long getSize() {
		return size;
	}




	public void setSize(long size) {
		this.size = size;
	}




	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(size);
		out.writeChar(seperator);
		out.writeInt(hits);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 this.size = in.readLong();
		 this.seperator = in.readChar();
		 this.hits = in.readInt();
		   

	}

	@Override
	public String toString() {
		return "" + hits +  seperator + size + "";
	}
	
	

}
