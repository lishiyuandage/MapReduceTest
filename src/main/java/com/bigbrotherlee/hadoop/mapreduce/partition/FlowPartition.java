package com.bigbrotherlee.hadoop.mapreduce.partition;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bigbrotherlee.hadoop.mapreduce.FlowBeanWritable;

public class FlowPartition {
	
	public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBeanWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 拿一行数据
			String line = value.toString();
			// 切分成各个字段
			String[] fields = StringUtils.split(line, "\t");

			// 拿到我们需要的字段
			String phoneNB = fields[1];
			long u_flow = Long.parseLong(fields[7]);
			long d_flow = Long.parseLong(fields[8]);

			// 封装数据为kv并输出
			context.write(new Text(phoneNB), new FlowBeanWritable(phoneNB, u_flow, d_flow));
		}
	}
public static class FlowSumAreaReducer extends Reducer<Text, FlowBeanWritable, Text, FlowBeanWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<FlowBeanWritable> values,Reducer<Text, FlowBeanWritable, Text, FlowBeanWritable>.Context context)
				throws IOException, InterruptedException {
			long up_flow_counter = 0;
			long d_flow_counter = 0;
			
			for(FlowBeanWritable bean: values){
				up_flow_counter += bean.getU_flow();
				d_flow_counter += bean.getD_flow();
			}
			context.write(key, new FlowBeanWritable(key.toString(), up_flow_counter, d_flow_counter));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowPartition.class);
		
		job.setMapperClass(FlowSumAreaMapper.class);
		job.setReducerClass(FlowSumAreaReducer.class);
		
		//设置我们自定义的分组逻辑定义
		job.setPartitionerClass(FlowPartitioner.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBeanWritable.class);
		
		//设置reduce的任务并发数，应该跟分组的数量保持一致
		job.setNumReduceTasks(6);
		
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
	
}
