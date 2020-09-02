/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tnas.hadoop.cases.wordcount;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author thiago
 */
public class ContadorPalavrasLocal {
    
    public static void main(String[] args) throws Exception {
        
        IntSumReducer reduceDriver = new IntSumReducer();
        
        List<IntWritable> valores = new ArrayList<>();
        valores.add(new IntWritable(7));
        valores.add(new IntWritable(5));
        valores.add(new IntWritable(8));
        
        reduceDriver.reduce(new Text("Sagah"), valores, null);
    }
}
