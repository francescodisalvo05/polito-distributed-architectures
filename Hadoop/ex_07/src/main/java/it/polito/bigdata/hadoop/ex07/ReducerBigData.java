package it.polito.bigdata.hadoop.ex07;

import java.io.IOException;

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

        String indices = new String();
        
        for(Text value : values) {
        	
        	if (indices.length() == 0)
        		indices = new String(value.toString());
        	
        	else 
        		indices = indices.concat("," + value.toString());
        	
        }
        
        context.write(new Text(key), new Text(indices));
    }
}
