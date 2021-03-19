package com.mr.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * className: FlowBeanMapper
 * description:
 * date: 2021/3/19 11:38
 *
 * @author yan
 */
public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    //8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
    private Text out_key = new Text();
    private FlowBean out_value = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        //获取手机号
        out_key.set(words[1]);
        //获取上行流量
        out_value.setUpFlow(Long.parseLong(words[words.length-3]));
        //获取下行流量
        out_value.setDownFlow(Long.parseLong(words[words.length-2]));
        context.write(out_key,out_value);
    }
}
