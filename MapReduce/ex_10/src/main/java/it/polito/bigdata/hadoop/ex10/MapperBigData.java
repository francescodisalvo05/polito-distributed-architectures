package it.polito.bigdata.hadoop.ex10;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// DO NOT FORGET TO IMPORT IT 
import it.polito.bigdata.hadoop.ex10.DriverBigData.MY_COUNTERS;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    NullWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	// each mapper call will just increment the counter	
    	context.getCounter(MY_COUNTERS.RECORD_COUNTER).increment(1);

            
    }
}
