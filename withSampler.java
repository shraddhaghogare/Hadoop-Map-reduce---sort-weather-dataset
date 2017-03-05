import org.apache.hadoop.mapreduce.lib.partition.InputSampler;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class withSampler
{

	public static class Mapper1 extends Mapper<DoubleWritable, Text, DoubleWritable, Text>
	{
		@Override
		public void map(DoubleWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
				//double t=Double.parseDouble(key);				
				context.write(key, value);
    		}
  	}




	public static class WordMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
			//int i=0;	    		
			String str = value.toString();						
			if(!str.contains(","))
			{
				//for (String s : strList)
				//i=str.getIndex();					
				String temp=str.substring(26,32).trim();
				double t=Double.parseDouble(temp);								
				context.write(new DoubleWritable(t), value);
    			}
    		}
  	}
  
  	public static class WordReducer extends Reducer<DoubleWritable, Text, Text, Text>
	{
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
						
			//int count = 0;
			for (Text val : values)
				//count += Integer.parseInt(val.toString());
				if(!(Double.parseDouble(key.toString())<0)){
				 if(Double.parseDouble(key.toString())!=9999.9)
					context.write(new Text(""+key), val);
				}
	    	}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) { 
			System.err.println("Usage: TotalOrderSorting <user data> <out> <sample rate>");
			System.exit(1);
		}
		Path inputPath = new Path(otherArgs[0]);
		Path partitionFile = new Path(otherArgs[1] + "_partitions.lst");
		Path outputStage = new Path(otherArgs[1] + "_staging");
		Path outputOrder = new Path(otherArgs[1]);
		double sampleRate = Double.parseDouble(otherArgs[2]);
		FileSystem.get(new Configuration()).delete(outputOrder, true);
		FileSystem.get(new Configuration()).delete(outputStage, true);
		FileSystem.get(new Configuration()).delete(partitionFile, true); 

		// Configure job to prepare for sampling
		Job sampleJob = new Job(conf, "Weather Sampler");
		sampleJob.setJarByClass(withSampler.class);
		// Use the mapper implementation with zero reduce tasks
		sampleJob.setMapperClass(WordMapper.class);
		sampleJob.setNumReduceTasks(0);
		sampleJob.setOutputKeyClass(DoubleWritable.class);
		sampleJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(sampleJob, inputPath); 
		// Set the output format to a sequence file
		sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);
		// Submit the job and get completion code.
		int code = sampleJob.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			Job orderJob = new Job(conf, "Weather sampler");
			orderJob.setJarByClass(withSampler.class);
		// Here, use the identity mapper to output the key/value pairs in
		// the SequenceFile
		orderJob.setMapperClass(Mapper1.class);
		orderJob.setReducerClass(WordReducer.class);
		
 		// Set the number of reduce tasks to an appropriate number for the
		// amount of data being sorted
		orderJob.setNumReduceTasks(3);
		// Use Hadoop's TotalOrderPartitioner class
		orderJob.setPartitionerClass(TotalOrderPartitioner.class);
		
		// Set the partition file
		TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),partitionFile);
		orderJob.setOutputKeyClass(DoubleWritable.class);
		orderJob.setOutputValueClass(Text.class);

		// Set the input to the previous job's output
		orderJob.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

		// Set the output path to the command line parameter
		TextOutputFormat.setOutputPath(orderJob, outputOrder);
		// Set the separator to an empty string
		orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");
		// Use the InputSampler to go through the output of the previous
		// job, sample it, and create the partition file
		//InputSampler.writePartitionFile(orderJob,new InputSampler.RandomSampler<DoubleWritable,Text>(3, 10000));
		InputSampler.Sampler<DoubleWritable, Text> sampler = new InputSampler.RandomSampler<DoubleWritable, Text>(sampleRate, 1000);
		InputSampler.writePartitionFile(orderJob, sampler);		
		// Submit the job
		code = orderJob.waitForCompletion(true) ? 0 : 2;
		} 
		// Cleanup the partition file and the staging directory
		FileSystem.get(new Configuration()).delete(partitionFile, false);
		FileSystem.get(new Configuration()).delete(outputStage, true);
		System.exit(code);
	}
}
