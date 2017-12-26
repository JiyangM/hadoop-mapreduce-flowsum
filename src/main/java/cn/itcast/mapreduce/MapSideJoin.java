package cn.itcast.mapreduce;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

/**
 * Map side join是针对以下场景进行的优化：两个待连接表中，有一个表非常大，而另一个表非常小，以至于小表可以直接存放到内存中。这样，我们可以将小表复制多份，让每个map task内存中存在一份（比如存放到hash table中），然后只扫描大表：对于大表中的每一条记录key/value，在hash table中查找是否有相同的key的记录，如果有，则连接后输出即可。
 * 为了支持文件的复制，Hadoop提供了一个类DistributedCache，使用该类的方法如下：
 * （1）用户使用静态方法DistributedCache.addCacheFile()指定要复制的文件，它的参数是文件的URI（如果是HDFS上的文件，可以这样：hdfs://namenode:9000/home/XXX/file，其中9000是自己配置的NameNode端口号）。JobTracker在作业启动之前会获取这个URI列表，并将相应的文件拷贝到各个TaskTracker的本地磁盘上。（2）用户使用DistributedCache.getLocalCacheFiles()方法获取文件目录，并使用标准的文件读写API读取相应的文件。
 * <p>
 * Created by jiyang on 18:45 2017/12/25
 */
public class MapSideJoin {

    public static Hashtable<String, String> joinData = new Hashtable<String, String>();


    static class JoinMapper implements Mapper {

        @Override
        public void configure(JobConf conf) {
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    String[] tokens;
                    BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    try {
                        while ((line = joinReader.readLine()) != null) {
                            tokens = line.split("\\t", 2);
                            joinData.put(tokens[0], tokens[1]);
                        }
                    } finally {
                        joinReader.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache: " + e);
            }
        }

        @Override
        public void map(Object o, Object value, OutputCollector output, Reporter reporter) throws IOException {
            Text t = (Text) value;
            String[] fields = t.toString().split("\\t");
            String address = joinData.get(fields[2]);
            if (address != null) {
                output.collect(new Text(fields[2]), new Text(fields[0] + "\t" + fields[1] + "\t" + address));
            }
        }

        @Override
        public void close() {

        }
    }

}