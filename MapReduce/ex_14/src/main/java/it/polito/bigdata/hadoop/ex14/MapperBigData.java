package it.polito.bigdata.hadoop.ex14;

import java.io.IOException;

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
                    NullWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		
    		String[] words = value.toString().split(",");
    		
    		for(int i = 0; i < words.length; i++) {
    			context.write(new Text(words[i].toLowerCase()), NullWritable.get());
    		}
    		
    		

            
    }
}
