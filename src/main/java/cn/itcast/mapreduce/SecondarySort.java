package cn.itcast.mapreduce;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 二次排序
 * Created by jiyang on 15:51 2017/12/25
 */
public class SecondarySort {

    static class SortMapper extends Mapper<LongWritable, Text, SortBean, SortBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            SortBean sb = new SortBean(Long.parseLong(line[0]), Long.parseLong(line[1]));
            context.write(sb, sb);
        }
    }

    static class SortReducer extends Reducer<SortBean, SortBean, NullWritable, SortBean> {

        @Override
        protected void reduce(SortBean key, Iterable<SortBean> values, Context context) throws IOException, InterruptedException {
            for (SortBean sb : values) {
                context.write(NullWritable.get(), sb);
            }
        }
    }

    // 自定义分区
    static class myPartitioner extends Partitioner<SortBean, NullWritable> {

        @Override
        public int getPartition(SortBean key, NullWritable var2, int numPartitions) {
            return key.getFirst().hashCode() % numPartitions;
        }
    }

    // 自定义groupComparator 实现 分区内排序
    @SuppressWarnings("rawtypes")
    public static class GroupingComparator extends WritableComparator{
        protected GroupingComparator(){
            super(SortBean.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            SortBean ip1 = (SortBean) w1;
            SortBean ip2 = (SortBean) w2;
            long l = ip1.getFirst();
            long r = ip2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\work\\soft\\hadoop-2.6.1");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(SortBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SortBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\work\\doc\\sort.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\work\\doc\\output"));

        job.setPartitionerClass(myPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }

}
