package it.polito.bigdata.hadoop.ex23;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * MapReduce program
 */
public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {
	
	int numReducers;
    Path inputPath;
    Path tempDir;
    Path outputDir;
	int exitCode;  
	
	
	numReducers = Integer.parseInt(args[0]);
    // Folder containing the input data
    inputPath = new Path(args[1]);
    // Output folder
    outputDir = new Path(args[2]);
    // Temp dir 
    tempDir = new Path("exTempFolder");
    
    
    Configuration conf = this.getConf();
    
    conf.set("user", args[3]);

    // Define a new job
    Job job1 = Job.getInstance(conf); 

    // Assign a name to the job
    job1.setJobName("Ex 23 - Job1");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job1, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job1, tempDir);
    
    // Specify the class of the Driver for this job
    job1.setJarByClass(DriverBigData.class);
    
    // Set job input format
    job1.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job1.setOutputFormatClass(TextOutputFormat.class);
       
    // Set map class
    job1.setMapperClass(MapperBigDataJob1.class);
    
    // Set map output key and value classes
    // Put the friends all together
    job1.setMapOutputKeyClass(NullWritable.class);
    job1.setMapOutputValueClass(Text.class);
    
    // Set reduce class
    job1.setReducerClass(ReducerBigDataJob1.class);
        
    // Set reduce output key and value classes
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(NullWritable.class);
    job1.setNumReduceTasks(numReducers);
    
    // Execute the job and wait for completion
    if (job1.waitForCompletion(true)==true) {
    	
    	Configuration conf2 = this.getConf();
    	conf2.set("user", args[3]);

        // Define a new job
        Job job2 = Job.getInstance(conf2); 

        
        // add the file in cache
        job2.addCacheFile(new Path(tempDir + "/part-r-00000").toUri());
        
        // Assign a name to the job
        job2.setJobName("Ex 23 - Job2");
        
        FileInputFormat.addInputPath(job2, inputPath);

		// Set path of the output folder for this job
		FileOutputFormat.setOutputPath(job2, outputDir);

		// Specify the class of the Driver for this job
		job2.setJarByClass(DriverBigData.class);
        
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
           
        // Set map class
        job2.setMapperClass(MapperBigDataJob2.class);
        
        // Set map output key and value classes
        // Put the friends all together
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        // Set reduce class
        job2.setReducerClass(ReducerBigDataJob2.class);
            
        // Set reduce output key and value classes
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        
        job2.setNumReduceTasks(numReducers);
        
     
    	
    	if (job2.waitForCompletion(true)==true)
        	exitCode=0;
        else
        	exitCode=1;
  } else
    	exitCode=1;
    	
    return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), 
    		new DriverBigData(), args);

    System.exit(res);
  }
}