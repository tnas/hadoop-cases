/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.avg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author thiago
 */
public class MediaMapReduceTeste {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private Text linha1 = new Text("1979 23 23 2 43 24 25 26 26 26 26 25 26 25");
    private Text linha2 = new Text("1980 26 27 28 28 28 30 31 31 31 30 30 30 29");
    private Text linha3 = new Text("1981 31 32 32 32 33 34 35 36 36 34 34 34 34");
    private Text linha4 = new Text("1984 39 38 39 39 39 41 42 43 40 39 38 38 40");
    private Text linha5 = new Text("1985 38 39 39 39 39 41 41 41 00 40 39 39 45");

    @Before
    public void setUp() {
        MediaMapper mapper = new MediaMapper();
        MediaReducer reducer = new MediaReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testarMapper() throws IOException {

        mapDriver.withInput(new LongWritable(0), linha1)
                .withInput(new LongWritable(0), linha2)
                .withInput(new LongWritable(0), linha3)
                .withInput(new LongWritable(0), linha4)
                .withInput(new LongWritable(0), linha5)
                .withOutput(new Text("1979"), new IntWritable(25))
                .withOutput(new Text("1980"), new IntWritable(29))
                .withOutput(new Text("1981"), new IntWritable(34))
                .withOutput(new Text("1984"), new IntWritable(40))
                .withOutput(new Text("1985"), new IntWritable(45))
                .runTest();
    }

    @Test
    public void testarReducer() throws IOException {

        List<IntWritable> valores1979 = new ArrayList<>();
        valores1979.add(new IntWritable(25));

        List<IntWritable> valores1980 = new ArrayList<>();
        valores1980.add(new IntWritable(29));

        List<IntWritable> valores1981 = new ArrayList<>();
        valores1981.add(new IntWritable(34));

        List<IntWritable> valores1984 = new ArrayList<>();
        valores1984.add(new IntWritable(40));

        List<IntWritable> valores1985 = new ArrayList<>();
        valores1985.add(new IntWritable(45));

        reduceDriver
                .withInput(new Text("1979"), valores1979)
                .withInput(new Text("1980"), valores1980)
                .withInput(new Text("1981"), valores1981)
                .withInput(new Text("1984"), valores1984)
                .withInput(new Text("1985"), valores1985)
                .withOutput(new Text("1981"), new IntWritable(34))
                .withOutput(new Text("1984"), new IntWritable(40))
                .withOutput(new Text("1985"), new IntWritable(45))
                .runTest();
    }

    @Test
    public void testarMapReduce() throws IOException {

        mapReduceDriver
                .withInput(new LongWritable(0), linha1)
                .withInput(new LongWritable(0), linha2)
                .withInput(new LongWritable(0), linha3)
                .withInput(new LongWritable(0), linha4)
                .withInput(new LongWritable(0), linha5)
                .withOutput(new Text("1981"), new IntWritable(34))
                .withOutput(new Text("1984"), new IntWritable(40))
                .withOutput(new Text("1985"), new IntWritable(45))
                .runTest();
    }
}
