package it.polito.bigdata.hadoop.ex20;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Mapper of a map-only job
 *  */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
	
	
	private MultipleOutputs<Text, NullWritable> mos = null;

	
	protected void setup(Context context) {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	
			String[] fields = value.toString().split(",");
			
			float temp =Float.parseFloat(fields[3]);
			
			if (temp>30.0)
				mos.write("hightemp", value.toString(), NullWritable.get());
			else
				mos.write("normaltemp", value.toString(), NullWritable.get());
				
    }
    
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

    
}
