package com.bigbrotherlee.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text word, Iterable<LongWritable> count,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long i=0;//计数器
		for(LongWritable longWritable:count) {
			i +=longWritable.get();
		}
		context.write(word, new LongWritable(i));
	}
}
