package cn.itcast.mapreduce;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * reduce side join是一种最简单的join方式，
 * 其主要思想如下：
 * 在map阶段，map函数同时读取两个文件File1和File2，为了区分两种来源的key/value数据对，对每条数据打一个标签（tag）,比如：tag=0表示来自文件File1，tag=2表示来自文件File2。即：map阶段的主要任务是对不同文件中的数据打标签。
 * 在reduce阶段，reduce函数获取key相同的来自File1和File2文件的value list， 然后对于同一个key，对File1和File2中的数据进行join（笛卡尔乘积）。即：reduce阶段进行实际的连接操作。
 * Created by jiyang on 17:54 2017/12/25
 */
public class ReduceSideJoin {

    static class JoinMapper extends Mapper<LongWritable, Text, UserInfoBean, UserInfoBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filePath = fileSplit.getPath().toString();
            UserInfoBean userInfoBean;
            if (filePath.contains("user1")) {
                userInfoBean = new UserInfoBean(Long.parseLong(line[0]), line[1], true);
            } else {
                userInfoBean = new UserInfoBean(Long.parseLong(line[0]), line[1], line[2], false);
            }
            context.write(userInfoBean, userInfoBean);
        }
    }

    // 自定义分区
    static class MyPartitioner extends Partitioner<UserInfoBean, NullWritable> {
        @Override
        public int getPartition(UserInfoBean userInfoBean, NullWritable nullWritable, int i) {
            return (int) (userInfoBean.getId() % i);
        }
    }

    static class JoinReducer extends Reducer<UserInfoBean, UserInfoBean, NullWritable, UserInfoBean> {
        @Override
        protected void reduce(UserInfoBean key, Iterable<UserInfoBean> values, Context context) throws IOException, InterruptedException {
            UserInfoBean ub = new UserInfoBean();
            for (UserInfoBean userInfoBean : values) {
                if (userInfoBean.isA()) {
                    ub.setId(userInfoBean.getId());
                    ub.setName(userInfoBean.getName());
                } else {
                    ub.setBdate(userInfoBean.getBdate());
                    ub.setEdate(userInfoBean.getEdate());
                }
            }
            context.write(NullWritable.get(), ub);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\work\\soft\\hadoop-2.6.1");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceSideJoin.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(UserInfoBean.class);
        job.setMapOutputValueClass(UserInfoBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(UserInfoBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\work\\doc\\sort.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\work\\doc\\output"));

        job.setPartitionerClass(MyPartitioner.class);

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }
}
