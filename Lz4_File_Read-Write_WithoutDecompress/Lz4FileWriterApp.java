package com.DXC.Lz4;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Lz4FileWriterApp 
{
    public static void main( String[] args ) throws Exception
    {
    	if(args.length !=2 ){
    		System.err.println("Usage : Lz4 File Writer Utility <input path> <output path>");
    		System.exit(-1);
    	}
    	Configuration conf = new Configuration();
    	Job job = new Job(conf);
    	job.setJarByClass(Lz4FileWriterApp.class);
    	job.setJobName("Lz4FileWriter");
    	
    	TextInputFormat.addInputPath(job,new Path(args[0]) );
    	TextOutputFormat.setOutputPath(job, new Path(args[1]));
  
    	job.setMapperClass(Lz4FileWriterMapper.class);
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	job.setNumReduceTasks(0);
    	
    	
    	System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
