package com.bigbrotherlee.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBeanWritable, Text, FlowBeanWritable> {
	@Override
	protected void reduce(Text key, Iterable<FlowBeanWritable> values,
			Reducer<Text, FlowBeanWritable, Text, FlowBeanWritable>.Context context)
			throws IOException, InterruptedException {
		
		long up=0;
		long down=0;
		
		for (FlowBeanWritable flowBeanWritable : values) {
			up+=flowBeanWritable.getU_flow();
			down+=flowBeanWritable.getD_flow();
		}
		
		context.write(key, new FlowBeanWritable(key.toString(), up, down));
	}
}
