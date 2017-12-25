package cn.itcast.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jiyang on 17:56 2017/12/25
 */
public class UserInfoBean implements Writable {

    private Long id;
    private String name;
    private String bdate;
    private String edate;
    private boolean isA;

    public UserInfoBean(Long id, String bdate, String edate, boolean isA) {
        this.id = id;
        this.bdate = bdate;
        this.edate = edate;
        this.isA = isA;
    }

    public UserInfoBean() {
    }

    public UserInfoBean(Long id, String name, boolean isA) {

        this.id = id;
        this.name = name;
        this.isA = isA;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeUTF(name);
        out.writeUTF(bdate);
        out.writeUTF(edate);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.id = input.readLong();
        this.name = input.readLine();
        this.bdate = input.readLine();
        this.edate = input.readLine();
    }

    @Override
    public String toString() {
        return id + " " + name + " " + bdate + " " + edate;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBdate() {
        return bdate;
    }

    public void setBdate(String bdate) {
        this.bdate = bdate;
    }

    public String getEdate() {
        return edate;
    }

    public void setEdate(String edate) {
        this.edate = edate;
    }

    public boolean isA() {
        return isA;
    }

    public void setA(boolean a) {
        isA = a;
    }
}
