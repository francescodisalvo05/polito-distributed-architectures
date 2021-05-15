package it.polito.bigdata.hadoop.ex17;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper1BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    DoubleWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		
    		String[] fields = value.toString().split(",");
    		
    		String date = fields[1];
    		Double temp = Double.parseDouble(fields[3]);
    		
    		System.out.println(temp + " \t " + date);
    		context.write(new Text(date), new DoubleWritable(temp));

            
    }
}
