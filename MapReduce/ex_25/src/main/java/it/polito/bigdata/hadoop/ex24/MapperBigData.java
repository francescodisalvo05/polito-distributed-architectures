package it.polito.bigdata.hadoop.ex24;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
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
                    Text> {// Output value type
	
	private ArrayList<String> friends;
	private String user;
    
    /*protected void setup(Context context) {
    	
    	friends = new ArrayList<String>();
    	user = context.getConfiguration().get("user").toString();
    	
    }*/
	
    protected void map(LongWritable key, 	// Input key type
			           Text value,         // Input value type
			           Context context) throws IOException, InterruptedException {
            	
	    	String[] friends = value.toString().split(",");
    		
    		
	    	// since they're friends, send each other
	    	// in both ways
    		context.write(new Text(friends[0]), new Text(friends[1]));
    		context.write(new Text(friends[1]), new Text(friends[0]));
        		
            
    }

	/*protected void cleanup(Context context) throws IOException, InterruptedException {
		
		
		String finalFriends = "";
		
		for (String f : friends) {
			finalFriends = finalFriends.concat(f + " ");
		}
		
		context.write(new Text(finalFriends), NullWritable.get());
		
	}*/
		
}
