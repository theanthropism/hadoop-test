package com.mr.flowbean;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * className: FlowBean
 * description:
 * date: 2021/3/19 11:44
 *
 * @author yan
 */
@Data
public class FlowBean implements Writable {

    /**上行流量*/
    private long upFlow;
    /**下行流量*/
    private long downFlow;
    /**总流量*/
    private long sumFlow;


    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
}
