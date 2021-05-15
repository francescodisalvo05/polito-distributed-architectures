package it.polito.bigdata.hadoop.ex17;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        double MAX = Double.MIN_VALUE;

        // Iterate over the set of values and sum them 
        for (DoubleWritable value : values) {
            if (value.get() > MAX)
            	MAX = value.get();
        }
        
        context.write(new Text(key), new DoubleWritable(MAX));
    }
}
