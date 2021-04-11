package it.polito.bigdata.hadoop.ex07;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		
    		String[] fields = value.toString().split("\\s+");
    		
    		for(String field : fields) {
    			
    			String word = field.toLowerCase();
    			
    			if (word.compareTo("and") != 0 && 
    				word.compareTo("or") != 0 && 
    				word.compareTo("not") != 0) {
    				
    				context.write(new Text(word), new Text(key));
    				
    			}
    		
    		}
    
            
    }
}
