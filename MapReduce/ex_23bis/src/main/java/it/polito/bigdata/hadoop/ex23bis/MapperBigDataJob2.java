package it.polito.bigdata.hadoop.ex23bis;

import java.io.IOException;
import java.util.ArrayList;

import java.io.BufferedReader;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigDataJob2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	

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
	
    protected void map(LongWritable key, 	// Input key type
			           Text value,         // Input value type
			           Context context) throws IOException, InterruptedException {
            
    		String[] users = value.toString().split(",");
    		
    		
    		// check if user 0 is in the list and if user 1 is not the input user
    		if(friends.contains(users[0]) == true && users[1].compareTo(user) != 0) 
    			context.write(NullWritable.get(), new Text(users[1]));
    		
    		// vice versa
    		if(friends.contains(users[1]) == true && users[0].compareTo(user) != 0) 
    			context.write(NullWritable.get(), new Text(users[0]));
    }


		
}
