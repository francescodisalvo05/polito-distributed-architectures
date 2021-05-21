package it.polito.bigdata.hadoop.ex24;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        ArrayList<String> friends = new ArrayList<String>();
        
        // Iterate over the set of values and sum them 
        for (Text value : values) {
            
        	if(friends.contains(value.toString()) == false) {
        		friends.add(value.toString());
        	}
            	
        }
        
        String friendsString = new String("");
        
        for (String friend : friends) {
        	friendsString = friendsString.concat(friend + " ");
        }	
        
        
        context.write(new Text(key), new Text(friendsString));
    }
}
