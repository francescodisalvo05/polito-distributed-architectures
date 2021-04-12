package it.polito.bigdata.hadoop.ex13bis;

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
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	
	DateValue top1;
	DateValue top2;
	
	protected void setup(Context context) {
		top1 = null;
		top2 = null;
	}
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		int currIncome = Integer.parseInt(value.toString());
    		String currKey = key.toString();
    		

    		// update top1 
    		if (top1 == null || currIncome > top1.value || (currIncome == top1.value && 
    				                                        currKey.compareTo(top1.date) <0 )) {
    			
    			// shift top1 to top2
    			top2 = top1;
    			
    			top1 = new DateValue();
    			top1.date = currKey;
    			top1.value = currIncome;
    			
    		}    else if (top2 == null || currIncome < top2.value || (currIncome == top2.value && 
                    												  currKey.compareTo(top2.date) <0 )) {
    			
    			top2 = new DateValue();
    			top2.date = currKey;
    			top2.value = currIncome;
    		}            
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	
		context.write(NullWritable.get(), new Text(top1.date + "_" + top1.value));
		context.write(NullWritable.get(), new Text(top2.date + "_" + top2.value));
	
    }
}
