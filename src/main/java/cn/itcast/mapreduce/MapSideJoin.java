package cn.itcast.mapreduce;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

/**
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
        public void map(Object o, Object o2, OutputCollector outputCollector, Reporter reporter) {

        }

        @Override
        public void close() {

        }
    }
}
