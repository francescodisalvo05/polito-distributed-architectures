package it.polito.bigdata.hadoop.ex23bis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigDataJob2 extends Reducer<
				NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
	
	private ArrayList<String> friends;
	private String user;
	
	protected void setup(Context context) throws IOException {
    	
    	user = context.getConfiguration().get("user").toString();
    	
    	// read the friends from the cached list and save in an array
    	friends = new ArrayList<String>();
    	
    	URI[] CachedFiles = context.getCacheFiles();

		// This application has one single cached file.
		// new Path(CachedFiles[0].getPath()).getName() is the name of the
		// shared file (i.e., part-r-00000 in this application).
		
		
		
		BufferedReader fileFriends = new BufferedReader(new FileReader(new File("exTempFolder/part-r-00000"))); 

		String line;
		// There is one friend per line
		while ((line = fileFriends.readLine()) != null) {
			friends.add(line);
		}

		fileFriends.close();
    }

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
    		if(friends.contains(friend) == false)
    			new_friend_string = new_friend_string.concat(" " + friend);
    	}
    	
    	context.write(new Text(new_friend_string), NullWritable.get());
        

        
        
    }
}
