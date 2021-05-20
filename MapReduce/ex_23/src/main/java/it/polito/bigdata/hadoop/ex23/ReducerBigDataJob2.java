package it.polito.bigdata.hadoop.ex23;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigDataJob2 extends Reducer<
				NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        // in order to check if the potential friends are unique
    	// add them in a list, and check every time
    	
    	ArrayList<String> potentialFriends = new ArrayList<String>();
    	
    	for(Text value : values) 
    		if(potentialFriends.contains(value.toString()) == false)
    			potentialFriends.add(value.toString());
    	
    	
    	String new_friend_string = "";
    	
    	for (String friend : potentialFriends) {
    		new_friend_string = new_friend_string.concat(friend + " ");
    	}
    	
    	context.write(new Text(new_friend_string), NullWritable.get());
        

        
        
    }
}
