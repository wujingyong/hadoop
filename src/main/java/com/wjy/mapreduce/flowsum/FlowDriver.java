package com.wjy.mapreduce.flowsum;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author wjy
 */
public class FlowDriver {
    @SneakyThrows
    public static void main(String[] args) {
        args = new String[]{"E:/hadoopTemp", "E:/hadoopTemp/output"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapperClass(FlowMap.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowMap.class);
        job.setReducerClass(FlowReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
