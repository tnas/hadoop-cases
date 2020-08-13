/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Thiago Nascimento - nacimenthiago@gmail.com
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
