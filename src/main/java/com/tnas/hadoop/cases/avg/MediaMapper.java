/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.avg;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author thiago
 */
public class MediaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String ultimoToken = null;
        StringTokenizer s = new StringTokenizer(value.toString());
        String year = s.nextToken();

        while (s.hasMoreTokens()) {
            ultimoToken = s.nextToken();
        }

        int media = Integer.parseInt(ultimoToken);
        context.write(new Text(year), new IntWritable(media));
    }

}
