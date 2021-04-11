package it.polito.bigdata.hadoop.ex11;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                SumCount,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<SumCount> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	float sum = 0;
        int counter = 0;
        
    	// there will be just one element for each sensor
    	// since it is iterable, we need to use the for anyway
    	for (SumCount value : values) {
    		sum += value.getSum();
    		counter += value.getCount();
    	}
        
        
        context.write(new Text(key), new FloatWritable((sum/counter)));
    }
}
