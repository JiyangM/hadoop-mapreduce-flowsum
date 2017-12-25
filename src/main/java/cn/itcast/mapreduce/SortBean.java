package cn.itcast.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by jiyang on 15:56 2017/12/25
 */
public class SortBean implements WritableComparable<SortBean> {

    private Long first;
    private Long second;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readLong();
        this.second = in.readLong();
    }


    @Override
    public int compareTo(SortBean sortBean){
        return (int) (this.first - sortBean.getFirst());
    }


    public Long getFirst() {
        return first;
    }

    public void setFirst(Long first) {
        this.first = first;
    }

    public Long getSecond() {
        return second;
    }

    public void setSecond(Long second) {
        this.second = second;
    }

    public SortBean(Long first, Long second) {
        this.first = first;
        this.second = second;
    }
}
