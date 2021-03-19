package com.mr.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * className: FlowBeanReducer
 * description:
 * date: 2021/3/19 12:43
 *
 * @author yan
 */
public class FlowBeanReducer extends Reducer<Text,FlowBean, Text,FlowBean> {
    private FlowBean out_value = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean flowBean : values){
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }
        out_value.setUpFlow(sumUpFlow);
        out_value.setDownFlow(sumDownFlow);
        out_value.setSumFlow(sumDownFlow+sumUpFlow);

        context.write(key,out_value);
    }
}
