package it.polito.bigdata.hadoop.ex11;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
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
                    SumCount> {// Output value type
    
	HashMap<String, SumCount> statistics;

	protected void setup(Context context) {
		statistics = new HashMap<String, SumCount>();
	}
	
	protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		
		SumCount localStats;
		
		String[] fields = value.toString().split(",");
		
		String sId = fields[0];
		float measure = Float.parseFloat(fields[2]);
		
		localStats = statistics.get(sId);
		
		// first time for this sensor
		if (localStats == null) {
			// instantiate
			localStats = new SumCount();
			
			// define the values
			localStats.setCount(1);
			localStats.setSum(measure);
			
			// put in the hashmap
			statistics.put(sId, localStats);
		} else {
			// update the values
			localStats.setCount(1 + localStats.getCount());
			localStats.setSum(measure + localStats.getSum());
			
			// overwrite
			statistics.put(new String(sId),localStats);
		}
            
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for(Entry<String,SumCount> pair : statistics.entrySet()) {
			
			context.write(new Text(pair.getKey()), pair.getValue());
			
		}
		
	}
}
