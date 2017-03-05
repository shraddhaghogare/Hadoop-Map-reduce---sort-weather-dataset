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

public class withPartitioner
{
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
			if(!(Double.parseDouble(key.toString())<0)){
				 if(Double.parseDouble(key.toString())!=9999.9)
				//count += Integer.parseInt(val.toString());
				context.write(new Text(""+key), val);
			}
	    	}
	}

//Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < DoubleWritable, Text >
   {
      @Override
      public int getPartition(DoubleWritable key, Text value, int numReduceTasks)
      {
         //String[] str = value.toString().split(" ");
         double temp = Double.parseDouble(key.toString());
         
 	 double maxTemp=999.99;
 	 double minTemp=0.17;


	 double maxLoad=0;
	 maxLoad=(maxTemp-minTemp)/numReduceTasks;
	 
	 //System.out.println("minLoad in partitioner:"+minTemp);
	 //System.out.println("maxtemp in partitioner:"+maxTemp);
	// System.out.println("maxLoad in partitioner:"+maxLoad);
	 if(numReduceTasks == 0)
         {
            return 0;
         }
	 if(temp>=minTemp && temp<(maxLoad/5))
         {
            return 0;
         }
         
         if(temp>= (maxLoad/5) && temp<(maxLoad/4))
 	 {
            return 1;
         }
	 if(temp>= maxLoad/4 && temp<=maxLoad)
	 {
	    return 2;
	 } 
	 else
		return 2;
      }

   }

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job j1 = new Job(conf);

		j1.setJobName("Weather job");
		j1.setJarByClass(withPartitioner.class);

		//Mapper output key and value type
		j1.setMapOutputKeyClass(DoubleWritable.class);
		j1.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(Text.class);

		//file input and output of the whole program
		j1.setInputFormatClass(TextInputFormat.class);
		j1.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the mapper class
		j1.setMapperClass(WordMapper.class);
		
		//set the partitioner class for custom partitioner
		j1.setPartitionerClass(CaderPartitioner.class);
		//set the combiner class for custom combiner
//		j1.setCombinerClass(LogReducer.class);

		//Set the reducer class
		j1.setReducerClass(WordReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j1.setNumReduceTasks(3);

		FileOutputFormat.setOutputPath(j1, new Path(args[1]));	
		FileInputFormat.addInputPath(j1, new Path(args[0]));

		j1.waitForCompletion(true);

 	}
}
