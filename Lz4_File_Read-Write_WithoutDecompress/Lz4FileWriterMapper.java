package com.DXC.Lz4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Lz4FileWriterMapper extends Mapper<LongWritable,Text,LongWritable, Text> {
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context) 	throws IOException, InterruptedException {
		context.write(key, value);		
	}

}
