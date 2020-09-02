/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.avg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author thiago
 */
public class MediaReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    private static final int MEDIA_LIMITE = 30;
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
         for (IntWritable val: values) { 
            if (val.get() > MEDIA_LIMITE) { 
               context.write(key, new IntWritable(val.get())); 
            } 
         }
    }
    
}
