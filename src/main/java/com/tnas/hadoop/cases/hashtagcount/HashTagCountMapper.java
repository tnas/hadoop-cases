/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.hashtagcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Thiago Nascimento - nacimenthiago@gmail.com
 */
public class HashTagCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    private final String hashtagRegExp = "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)";

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String[] words = value.toString().split(" ");

        for (String str: words) {
            if (str.matches(hashtagRegExp)) {
                word.set(str);
                context.write(word, one);
            }
        }
    }
}
