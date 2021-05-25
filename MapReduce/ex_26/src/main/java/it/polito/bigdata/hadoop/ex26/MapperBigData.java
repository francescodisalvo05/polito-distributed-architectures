package it.polito.bigdata.hadoop.ex26;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import java.net.URI;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	private HashMap<String,Integer> dictionary;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		
		dictionary = new HashMap<String,Integer>();
		
		// read the file from the cache
		URI[] urisCachedFiles = context.getCacheFiles();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(urisCachedFiles[0].getPath())));
		
		String line = "";
		
		while( (line = file.readLine()) != null) {
			
			String[] fields = line.split("\t");
			dictionary.put(fields[1], Integer.parseInt(fields[0]));
			
		}
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    				// final string made by the tokens
    				// taken from the dictionary
    				String finalString = new String("");
    				// current integer token
    				int integerToken;
    				
    				String[] tokens = value.toString().split(" ");
    				
    				for (String token : tokens) {
    					
    					// get the key from the element
    					integerToken = dictionary.get(token.toUpperCase());
    					
    					// concatenate
    					finalString = finalString.concat(integerToken + " ");
    					
    				}
    				
    				context.write(NullWritable.get(), new Text(finalString));
            }
   }

