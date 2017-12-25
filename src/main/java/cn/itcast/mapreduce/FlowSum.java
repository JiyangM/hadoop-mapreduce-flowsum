package cn.itcast.mapreduce;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FlowSum {

	// 在kv中传输我们的自定义的对象是可以的 ，不过必须要实现hadoop的序列化机制 也就是implement Writable
	public static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

		Text k = new Text();
		FlowBean v = new FlowBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 将读取到的每一行数据进行字段的切分
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");

			// 抽取我们业务所需要的字段
			String phoneNum = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long downFlow = Long.parseLong(fields[fields.length - 2]);

			k.set(phoneNum);
			v.set(upFlow, downFlow);

			context.write(k, v);

		}
	}

	public static class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

		FlowBean v = new FlowBean();

		// 这里reduce方法接收到的key就是某一组《a手机号，bean》《a手机号，bean》
		// 《b手机号，bean》《b手机号，bean》当中的第一个手机号
		// 这里reduce方法接收到的values就是这一组kv对中的所以bean的一个迭代器
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {

			long upFlowCount = 0;
			long downFlowCount = 0;

			for (FlowBean bean : values) {
				upFlowCount += bean.getUpFlow();
				downFlowCount += bean.getDownFlow();
			}
			v.set(upFlowCount, downFlowCount);
			context.write(key, v);
		}
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir","D:\\work\\soft\\hadoop-2.6.1");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowSum.class);

		// 告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);

		// 告诉框架，我们程序输出的数据类型
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 告诉框架，我们程序使用的数据读取组件 结果输出所用的组件是什么
		// TextInputFormat是mapreduce程序中内置的一种读取数据组件 准确的说 叫做 读取文本文件的输入组件
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 告诉框架，我们要处理的数据文件在那个路劲下
	    // FileInputFormat.setInputPaths(job, new Path("/flowsum/input"));
		// 告诉框架，我们的处理结果要输出到什么地方
		// FileOutputFormat.setOutputPath(job, new Path("/flowsum/output"));
		
	    FileInputFormat.setInputPaths(job, new Path("D://jy//flowsum//input"));
		FileOutputFormat.setOutputPath(job, new Path("D://jy//flowsum//output"));

		boolean res = job.waitForCompletion(true);

		System.exit(res ? 0 : 1);
	}

}
