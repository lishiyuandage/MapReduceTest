package com.bigbrotherlee.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FlowSumRunner extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowSumRunner.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBeanWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBeanWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)?1:0;
	}
	
	public static void main(String[] args) throws Exception{
		int status=ToolRunner.run(new Configuration(), new FlowSumRunner(), args);
		System.exit(status);
	}
}
