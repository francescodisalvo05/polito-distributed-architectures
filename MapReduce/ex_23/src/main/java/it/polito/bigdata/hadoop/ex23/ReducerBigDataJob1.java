package it.polito.bigdata.hadoop.ex23;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigDataJob1 extends Reducer<
				NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        // Iterate over the set of values and sum them 
    	// I suppose there are no duplicated rows
        for (Text value : values) {
        	context.write(new Text(value), NullWritable.get());
        }
        

        
        
    }
}
