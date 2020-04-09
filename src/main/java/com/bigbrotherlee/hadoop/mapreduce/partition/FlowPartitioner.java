package com.bigbrotherlee.hadoop.mapreduce.partition;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
	private static HashMap<String, Integer> map=new HashMap<String, Integer>();
	
	static {
		map.put("134", 0);
		map.put("135", 1);
		map.put("136", 2);
		map.put("137", 4);
	}
	
	
	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {

		int areaCoder  = map.get(key.toString().substring(0, 3))==null?5:map.get(key.toString().substring(0, 3));

		return areaCoder;
	}

}
