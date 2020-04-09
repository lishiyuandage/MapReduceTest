package com.bigbrotherlee.hadoop.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBeanWritableComparable implements WritableComparable<FlowBeanWritableComparable>{
	private String phoneNum;
	private long u_flow;
	private long d_flow;
	private long s_flow;
	
	public  FlowBeanWritableComparable() {}

	public FlowBeanWritableComparable(String phoneNum, long u_flow, long d_flow) {
		super();
		this.phoneNum = phoneNum;
		this.u_flow = u_flow;
		this.d_flow = d_flow;
		this.s_flow=u_flow+d_flow;
	}

	public String getPhoneNum() {
		return phoneNum;
	}

	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
	}

	public long getU_flow() {
		return u_flow;
	}

	public void setU_flow(long u_flow) {
		this.u_flow = u_flow;
	}

	public long getD_flow() {
		return d_flow;
	}

	public void setD_flow(long d_flow) {
		this.d_flow = d_flow;
	}

	public long getS_flow() {
		return s_flow;
	}

	public void setS_flow(long s_flow) {
		this.s_flow = s_flow;
	}
	
	//hadoop序列化方法
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phoneNum);
		out.writeLong(u_flow);
		out.writeLong(d_flow);
		out.writeLong(s_flow);
	}
	//hadoop反序列化方法
	public void readFields(DataInput in) throws IOException {
		phoneNum=in.readUTF();
		u_flow=in.readLong();
		d_flow=in.readLong();
		s_flow=in.readLong();
	}

	@Override
	public String toString() {
		return "[电话号码=" + phoneNum + ",上行流量=" + u_flow + ", 下行流量=" + d_flow + ", 总流量="+ s_flow + "]";
	}

	public int compareTo(FlowBeanWritableComparable o) {
//		return 0;
		//排序
		return this.s_flow>o.getS_flow()?-1:1;
	}

}
