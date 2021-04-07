package it.polito.bigdata.hadoop.ex06;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    DoubleWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		
    		String[] fields = value.toString().split(",");
    		
    		String sensor = fields[0];
    		Double measure = Double.parseDouble(fields[2]);
    		
    		context.write(new Text(sensor), new DoubleWritable(measure));

            
    }
}
