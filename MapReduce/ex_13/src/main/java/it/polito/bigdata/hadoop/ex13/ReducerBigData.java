package it.polito.bigdata.hadoop.ex13;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,  // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String maxDate = null;
    	int maxIncome = Integer.MIN_VALUE;
        
        // Iterate over the set of values and sum them.
        // Count also the number of values
        for (Text value : values) {
        	
        	String[] fields = value.toString().split("_");
        	int currIncome = Integer.parseInt(fields[1]);
        	
        	if(currIncome > maxIncome || (currIncome == maxIncome && fields[0].compareTo(maxDate) < 0)) {
        		maxDate = fields[0];
        		maxIncome = Integer.parseInt(fields[1]);
        	}
        }

    	
        context.write(new Text(maxDate), new IntWritable(maxIncome));
    }
}
