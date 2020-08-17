/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author thiago
 */
public class ContadorPalavrasTest {

    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        TokenizerMapper mapper = new TokenizerMapper();
        IntSumReducer reducer = new IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testarMapper() throws IOException {
        Text valor = new Text("CSBC JAI 2012 CSBC");
        mapDriver.withInput(new IntWritable(0), valor)
                .withOutput(new Text("CSBC"), new IntWritable(1))
                .withOutput(new Text("JAI"), new IntWritable(1))
                .withOutput(new Text("2012"), new IntWritable(1))
                .withOutput(new Text("CSBC"), new IntWritable(1))
                .runTest();
    }
    
    @Test
    public void testarReducer() throws IOException {
        
        List<IntWritable> valoresCSBC = new ArrayList<>();
        valoresCSBC.add(new IntWritable(2));
        valoresCSBC.add(new IntWritable(1));
        
        List<IntWritable> valores2012 = new ArrayList<>();
        valores2012.add(new IntWritable(2));
        valores2012.add(new IntWritable(2));
        
        reduceDriver
                .withInput(new Text("CSBC"), valoresCSBC)
                .withInput(new Text("2012"), valores2012)
                .withOutput(new Text("CSBC"), new IntWritable(3))
                .withOutput(new Text("2012"), new IntWritable(4))
                .runTest();
    }
    

    public void testarMapReduce() throws IOException {
        
        mapReduceDriver
                .withInput(new IntWritable(), new Text("CSBC JAI 2012 CSBC 2012 CSBC"))
                .withOutput(new Text("2012"), new IntWritable(2))
                .withOutput(new Text("CSBC"), new IntWritable(3))
                .withOutput(new Text("JAI"), new IntWritable(1))
                .runTest();
    }
    
}
