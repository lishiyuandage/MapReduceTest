package com.bigbrotherlee.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 描述job
 * 指定map、reduce、输入输出等等
 * @author lee
 *
 */
public class WordCountRunner {
	public static void main(String[] args) throws Exception {
		//得到作业对象
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		//设置所用类的jar包
		job.setJarByClass(WordCountRunner.class);
		
		//设置map、reduce
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		//设置map和reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//设置数据目录和输出目录
		FileInputFormat.setInputPaths(job, new Path("/wc/input/"));
		FileOutputFormat.setOutputPath(job, new Path("/wc/output/"));
		
		//提交作业
		job.waitForCompletion(true);
	}
}
