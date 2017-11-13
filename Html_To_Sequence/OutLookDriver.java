// Decompiled by DJ v3.12.12.101 Copyright 2016 Atanas Neshkov  Date: 11/13/2017 9:44:50 AM
// Home Page:  http://www.neshkov.com/dj.html - Check often for new version!
// Decompiler options: packimports(3) 
// Source File Name:   OutlookDriver.java

package com.hp.it.cx;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

// Referenced classes of package com.hp.it.cx:
//            OutlookMapper

public class OutlookDriver
{

    public OutlookDriver()
    {
    }

    public static void main(String args[])
        throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        conf.set("EmailCategory", args[0]);
        Job job = new Job(conf, "Outlookmail");
        job.setJarByClass(OutlookDriver.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileSystem.get(conf).delete(new Path(args[2]), true);
        MultipleOutputs.addNamedOutput(job, "OutlookMail", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "ErrorOutlookMail", TextOutputFormat.class, Text.class, Text.class);
        job.setMapperClass(OutlookMapperMap.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

