package OutlookHTMLcleansing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class HTMLCleansingdriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
Configuration conf = new Configuration();
		
		Job job = new Job(conf, "HTMLCleansing");
		job.setJarByClass(HTMLCleansingdriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);;
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//FileSystem.get(conf).delete(new Path(args[2]), true);
	 
		
		job.setMapperClass(HTMLCleansingmapper.Map.class);
		
		job.waitForCompletion(true);

	}

}
