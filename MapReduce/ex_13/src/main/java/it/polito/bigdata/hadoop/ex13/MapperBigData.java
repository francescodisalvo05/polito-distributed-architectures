package it.polito.bigdata.hadoop.ex13;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	// collect the maximum number for each mapper
	HashMap<String,Integer> hashMap;
	
	protected void setup(Context context) {
		hashMap = new HashMap<String,Integer>();
		hashMap.put("default", Integer.MIN_VALUE);
	}
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		int currIncome = Integer.parseInt(value.toString());
    		String currKey = key.toString();
    		
    		// get the current maximum record
    		String maxKey = hashMap.entrySet().stream().findFirst().get().getKey();
    		Integer maxValue = hashMap.entrySet().stream().findFirst().get().getValue();
    		
    		if (currIncome > maxValue) {
    			
    			// remove the element from the hashmap and put the new one
    			hashMap.put(currKey,currIncome);
    			hashMap.remove(maxKey);
    			
    		}     		
            
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	
    	for (Entry<String,Integer> pair : hashMap.entrySet()) {
    		String out = pair.getKey() + "_" + pair.getValue();
    		context.write(NullWritable.get(), new Text(out));
    	}
    	
    }
}
