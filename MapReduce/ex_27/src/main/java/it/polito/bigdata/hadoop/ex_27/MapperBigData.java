package it.polito.bigdata.hadoop.ex_27;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

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
    
	ArrayList<String> businessRules;
	
	protected void setup(Context context) throws IOException {
		
		businessRules = new ArrayList<String>();
		
		// read file
		URI[] CachedFiles = context.getCacheFiles();
		BufferedReader file = new BufferedReader(new FileReader(new File(CachedFiles[0].getPath())));
		
		String line = "";
		
		while((line = file.readLine()) != null) {
			businessRules.add(line);
		}
		
		file.close();
		
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		// get the row
    		// e.g. User#1,John,Smith,M,1934,New York,Bachelor
    		String row = value.toString();
            String[] fields = row.split(",");
            
            // Output string with the obtained decision rule
            String outputRow = new String("");
            
            
            // string for comparing the business rule
            // that is the following form : 
            // |-- Gender=M and YearOfBirth=1934 -> Category#1
            String adjustedRow = "Gender=" + fields[3] + " and YearOfBirth=" + fields[4];
            
            // look for the match on the business rules
            for(String rule : businessRules) {
            	
            	String constraints = rule.split(" -> ")[0];
            	String decision = rule.split(" -> ")[1];
            	
            	if (adjustedRow.compareTo(constraints) == 0) {
            		
            		outputRow = outputRow.concat(row + "," + decision);
            		context.write(NullWritable.get(), new Text(outputRow));
            		return;
            		
            	}
            	
            }
            
            // if it arrives here, it means that there is no 
            // decision rule for the current row
            // so, use the "Unknown" class
            outputRow = outputRow.concat(row + ",Unknown");
    		context.write(NullWritable.get(), new Text(outputRow));
    		
    }
}
