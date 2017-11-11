package cn.itcast.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	//序列化框架在反序列化的时候创建对象的实例会去调用我们的无参构造函数
	public FlowBean() {
		
	}

	public FlowBean(long upFlow, long downFlow, long sumFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = sumFlow;
	}
	
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow+downFlow;
	}
	
	public void set(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow+downFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	
	//序列化的方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
		
	}

	//反序列化的方法
	//注意：字段的反序列化的顺序跟序列化的顺序必须保持一致
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow =in.readLong();
		this.sumFlow =in.readLong();
		
	}

	@Override
	public String toString() {
		return  upFlow + "\t" +downFlow + "\t" +sumFlow;
	}

	/**
	 * 这里进行我们自定义比较大小的规则
	 */
	@Override
	public int compareTo(FlowBean o) {
		
		return (int) (o.getSumFlow()-this.getSumFlow());
	}
	

}
