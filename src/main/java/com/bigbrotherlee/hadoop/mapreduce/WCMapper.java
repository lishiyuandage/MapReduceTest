package com.bigbrotherlee.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String [] words=StringUtils.split(value.toString(), " ");
		
		for(String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
		
	}
	
}
