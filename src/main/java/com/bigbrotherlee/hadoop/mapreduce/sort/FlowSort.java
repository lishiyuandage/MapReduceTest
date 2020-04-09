package com.bigbrotherlee.hadoop.mapreduce.sort;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSort {
	
	//mapper
	public static class SortMapper extends Mapper<LongWritable, Text, FlowBeanWritableComparable, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, FlowBeanWritableComparable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String [] param=StringUtils.split(line, "\t");
			
			String phoneNum=param[1];
			long up=Long.parseLong(param[7]);
			long down =Long.parseLong(param[8]);
			
			context.write(new FlowBeanWritableComparable(phoneNum,up,down),NullWritable.get());
			
		}
		
	}
	
	//reducer
	public static class SortReducer extends Reducer<FlowBeanWritableComparable, NullWritable, Text, FlowBeanWritableComparable>{
		
		@Override
		protected void reduce(FlowBeanWritableComparable key, Iterable<NullWritable> value,Reducer<FlowBeanWritableComparable, NullWritable, Text, FlowBeanWritableComparable>.Context context)
				throws IOException, InterruptedException {
			String phoneNB = key.getPhoneNum();
			context.write(new Text(phoneNB), key);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSort.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBeanWritableComparable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBeanWritableComparable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
}
