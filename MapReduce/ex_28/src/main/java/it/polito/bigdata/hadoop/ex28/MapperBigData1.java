package it.polito.bigdata.hadoop.ex28;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
	/*
	 *  MAPPING THE QUESTIONS
	 *  |-- Q1,2015-01-01,What is ..?
	 */
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		
            String[] fields = value.toString().split(",");
            
            // emit one key value pair, where
            // key is the "Id" of the question, that will be matched with the answer
            // value is the question's row
            context.write(new Text(fields[0]), new Text(value));
    		
    }
}
