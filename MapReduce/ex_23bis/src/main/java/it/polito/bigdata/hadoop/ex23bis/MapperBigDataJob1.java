package it.polito.bigdata.hadoop.ex23bis;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigDataJob1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	

	private String user;
    
    protected void setup(Context context) {
    	
    	user = context.getConfiguration().get("user").toString();
    	
    }
	
    protected void map(LongWritable key, 	// Input key type
			           Text value,         // Input value type
			           Context context) throws IOException, InterruptedException {
            
    		String[] potentialFriends = value.toString().split(",");
    		
    		
    		if (potentialFriends[1].equals(user))
    			context.write(NullWritable.get(), new Text(potentialFriends[0]));
    		
    		if (potentialFriends[0].equals(user))
    			context.write(NullWritable.get(), new Text(potentialFriends[1]));
        		
            
    }


		
}
