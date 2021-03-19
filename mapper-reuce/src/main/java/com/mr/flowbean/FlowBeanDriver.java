package com.mr.flowbean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * className: FlowBeanDriver
 * description:
 * date: 2021/3/19 12:53
 *
 * @author yan
 */
public class FlowBeanDriver {
    public static void main(String[] args)  throws Exception{
        Path inputPath=new Path("/mrinput/flowbean");
        Path outputPath=new Path("/wordcount2/flowean");

        //作为整个Job的配置
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://c1:9000");


        //保证输出目录不存在
        FileSystem fs=FileSystem.get(conf);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

        }
        // ①创建Job
        Job job = Job.getInstance(conf);

        // ②设置Job
        // 设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //声明使用NLineInputFormat(用于一行字段很多)
        //job.setInputFormatClass(NLineInputFormat.class);

        // 设置输入目录和输出目录
        NLineInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // ③运行Job
        job.waitForCompletion(true);

    }
}
