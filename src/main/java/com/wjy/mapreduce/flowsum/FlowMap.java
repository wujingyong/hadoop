package com.wjy.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wjy
 */
public class FlowMap extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text text = new Text();
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        int length = words.length;
        flowBean.setUpFlow(Long.valueOf(words[length - 3]));
        flowBean.setDownFlow(Long.valueOf(words[length - 2]));
        text.set(words[1]);
        context.write(text, flowBean);
    }
}
