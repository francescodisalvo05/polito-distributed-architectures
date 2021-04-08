package it.polito.bigdata.hadoop.ex08;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        double sum = 0.0;
        int counter = 0;

        // Iterate over the set of values and sum them 
        for (DoubleWritable value : values) {
            sum += value.get();
            counter += 1;
        }
        
        double avg_per_year = sum/counter;
        
        context.write(new Text(key), new DoubleWritable(avg_per_year));
    }
}
