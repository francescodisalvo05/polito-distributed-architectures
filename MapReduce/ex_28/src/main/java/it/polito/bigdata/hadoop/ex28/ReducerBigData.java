package it.polito.bigdata.hadoop.ex28;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
        String questionRow = new String("");
        ArrayList<String> answers = new ArrayList<String>();
        
    	for (Text value : values) {
    		
    		String[] fields = value.toString().split(",");
    		System.out.println("--> " + value.toString());
    		
    		// this is the question
    		if (fields[0].startsWith("Q") == true) {
    			questionRow = questionRow.concat(fields[0] + "," + fields[2]);
    		} 
    		
    		// this is an answer
    		else {
    			answers.add(fields[0] + "," + fields[3]);
    		}
    	}
    	
    	for (String answer : answers) {
    		
    		String out = questionRow + "," + answer;
    		
    		context.write(NullWritable.get(), new Text(out));
    	}
    	
    }
}
