package it.polito.bigdata.hadoop.ex12;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type
    
	float threshold;
	
	// get the threshold once for each mapper
	protected void setup(Context context) {
		threshold = Float.parseFloat(context.getConfiguration().get("threshold"));
	}
	
    protected void map(
    		Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
   
    	
    	float measure = Float.parseFloat(value.toString());
    	
    	
    	if (measure < threshold) {
    		context.write(new Text(key), new FloatWritable(measure));
    	}
    	
    	
    	
            
    }
}
