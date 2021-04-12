package it.polito.bigdata.hadoop.ex13bis;

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
    		
    		DateValue top1 = null;
    		DateValue top2 = null;
    		
    		int currIncome;
    		String date;
    		
    		for (Text value : values) {
    			
    			String[] fields = value.toString().split("_");
    			
    			date = fields[0];
    			currIncome = Integer.parseInt(fields[1]);
    			
    			if (top1==null || top1.value<currIncome) {
        			// shift
    				top2=top1;
        			
    				// update
        			top1 = new DateValue();
        			top1.date = date;
        			top1.value = currIncome;
        		}
        		else if (top2==null || top2.value < currIncome){
        				
					top2 = new DateValue();
					top2.date = date;
					top2.value = currIncome;
        				
        		}
    			
    		}
    	
	    	// Emit pair (date, value) top1
	        // Emit pair (date, value) top2
	        context.write(new Text(top1.date), new IntWritable(top1.value));
	        context.write(new Text(top2.date), new IntWritable(top2.value));
    	
        }

    	
    }
