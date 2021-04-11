package it.polito.bigdata.hadoop.ex04;

import java.io.IOException;

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
    
	private static Double threshold = new Double(50);
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] words = key.toString().split(",");
            
            String zone = words[0];
            String date = words[1];
            Double measure = Double.parseDouble(value.toString());
            
            if (measure > threshold) {
            	context.write(new Text(zone), new Text(date));
            }
            
            
    }
}
