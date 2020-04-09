package com.bigbrotherlee.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBeanWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBeanWritable>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String [] strs=StringUtils.split(line,"\t");
		String phone=strs[1];
		long up=Long.parseLong(strs[7]);
		long down=Long.parseLong(strs[8]);
		context.write(new Text(phone), new FlowBeanWritable(phone, up, down));
	}
}
