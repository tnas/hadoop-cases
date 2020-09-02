/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Thiago Nascimento - nacimenthiago@gmail.com
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        
        int sum = 0;
        for (IntWritable val: values) {
            sum += val.get();
        }
        
        result.set(sum);
        context.write(key, result);
        
//        result.set(sum % 2);
//        Text keyInfo = new Text("Chave: " + key.toString());
//        context.write(keyInfo, result);
        
    }
}
