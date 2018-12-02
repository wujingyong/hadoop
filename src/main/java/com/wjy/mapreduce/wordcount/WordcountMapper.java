package com.wjy.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wjy
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable intWritable = new IntWritable(1);
    private Text text = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
