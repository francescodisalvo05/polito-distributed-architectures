package it.polito.bigdata.hadoop.ex21;

import java.io.IOException;
import java.util.ArrayList;
import java.net.URI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper< LongWritable, // Input key type
									Text,         // Input value type
									NullWritable,         // Output key type
									Text> {// Output value type
    
	private ArrayList<String> stopWords;
	
	protected void setup(Context context) throws IOException {
		String nextLine;
    	
		// load the data on this ArrayList
		stopWords = new ArrayList<String>();
		URI[] urisCachedFiles = context.getCacheFiles();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(urisCachedFiles[0].getPath())));
		
		while((nextLine = file.readLine()) != null) {
			stopWords.add(nextLine);
		}
		
		file.close();
	}
	
    protected void map(
    		
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		// split in words
    		String[] words = value.toString().split("\\s+");
    		// define the new sentence
    		String cleanedSentence = new String("");
    		
    		boolean foundStopword = false;
    		
    		for(String word : words) {
    			
    			if(stopWords.contains(word.toLowerCase()))
    				continue;
    				
    			cleanedSentence = cleanedSentence.concat(word + " ");
    			
    		}
    		
    		context.write(NullWritable.get(), new Text(cleanedSentence));
            
    }
}
