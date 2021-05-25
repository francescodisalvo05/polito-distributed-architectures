package it.polito.bigdata.hadoop.ex28;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
	/*
	 *  MAPPING THE ANSWERS
	 *  |-- A1,Q1,2015-01-02,It is ..
	 */
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		
            String[] fields = value.toString().split(",");
            
            // emit one key value pair, where
            // key is the "Id" of the question, that will be matched with the original question
            // value is the answer's row
            context.write(new Text(fields[1]), new Text(value));
    		
    }
}
