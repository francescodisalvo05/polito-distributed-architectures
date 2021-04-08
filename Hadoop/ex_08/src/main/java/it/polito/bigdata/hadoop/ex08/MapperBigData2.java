package it.polito.bigdata.hadoop.ex08;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    DoubleWritable> {// Output value type
    
    protected void map(
    		Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		
    		String[] fields = key.toString().split("-");
    		
    		String year = fields[0];
    		Double income = Double.parseDouble(value.toString());
    		
    		context.write(new Text(year), new DoubleWritable(income));

            
    }
}
