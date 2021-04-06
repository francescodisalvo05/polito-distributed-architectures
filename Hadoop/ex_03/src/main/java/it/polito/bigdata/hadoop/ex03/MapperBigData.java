package it.polito.bigdata.hadoop.ex03;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
					Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
	private static Double threshold = new Double(50);
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] composed_key = key.toString().split(",");
            
            String id = composed_key[0];
            Double measure = Double.parseDouble(value.toString());
            
            if(measure > threshold ) {
            	context.write(new Text(id), NullWritable.get());
            }
            
           
    }
}
