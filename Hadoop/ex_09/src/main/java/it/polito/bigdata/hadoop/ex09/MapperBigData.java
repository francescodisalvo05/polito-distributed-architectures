package it.polito.bigdata.hadoop.ex09;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
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
                    IntWritable> {// Output value type
	
	HashMap<String,Integer> localWordCounts;
	
	// instantiate one hashmap for each mapper
	protected void setup(Context context) {
		localWordCounts = new HashMap<String,Integer>();
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		Integer freq;
    		
    		String[] words = value.toString().split("\\s+");
    		
    		for(String word : words) {
    			
    			String cleanedW = word.toLowerCase();
    			
    			freq = localWordCounts.get(cleanedW);
    			
    			// new words
    			if(freq == null)
    				localWordCounts.put(new String(cleanedW), 1);
    			// update
    			else 
    				localWordCounts.put(new String(cleanedW), freq + 1);
    				
    		}
            
    }
    
    // emit the values once you do not have any other information
    protected void cleanup(Context context) throws IOException, InterruptedException {
		
    	// Emit the set of (key, value) pairs of this mapper
		for (Entry<String, Integer> pair : localWordCounts.entrySet()) {
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
		}
    	
	}
}
