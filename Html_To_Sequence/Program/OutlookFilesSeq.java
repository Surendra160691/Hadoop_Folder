// Decompiled by DJ v3.12.12.101 Copyright 2016 Atanas Neshkov  Date: 11/13/2017 9:46:12 AM
// Home Page:  http://www.neshkov.com/dj.html - Check often for new version!
// Decompiler options: packimports(3) 
// Source File Name:   OutlookFilesSeq.java

package com.hp.it.cx;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class OutlookFilesSeq
{

    public OutlookFilesSeq()
    {
    }

    @SuppressWarnings("deprecation")
	public static void main(String args[])
        throws IOException
    {
        try
        {
            Configuration conf = new Configuration();
            //LocalFileSystem fs = FileSystem.getLocal(conf);
            FileSystem fs = FileSystem.get(conf);
            Path inputFile = new Path(args[1]);
            Path outputFile = new Path(args[2]);
            Text key = new Text();
            Text value = new Text();
            org.apache.hadoop.io.SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outputFile, key.getClass(), value.getClass());
            int filelimt = Integer.parseInt(args[0]);
            double totalsize = 0.0D;
            int filename = 0;
            FileStatus fStatus[] = fs.listStatus(inputFile);
            FileStatus afilestatus[];
            int j = (afilestatus = fStatus).length;
            for(int i = 0; i < j; i++)
            {
                FileStatus fst = afilestatus[i];
                File f = new File((new StringBuilder()).append(inputFile).append("/").append(fst.getPath().getName()).toString());
                totalsize += f.length();
                if(totalsize > (double)filelimt)
                {
                    totalsize = 0.0D;
                    writer.close();
                    IOUtils.closeStream(writer);
                    System.out.println("Status : SUCCESSFUL");
                    filename++;
                    outputFile = new Path((new StringBuilder(String.valueOf(args[2]))).append(Integer.toString(filename)).toString());
                    System.out.println((new StringBuilder("The output path is ")).append(outputFile.toString()).toString());
                    writer = SequenceFile.createWriter(fs, conf, outputFile, key.getClass(), value.getClass());
                }
                System.out.println((new StringBuilder("Processing file : ")).append(fst.getPath().getName()).append(" and the size is : ").append(f.length()).toString());
                InputStream inputStream = fs.open(fst.getPath());
                byte bytes[] = toByteArrayUsingJava(inputStream);
                key.set(fst.getPath().getName().toString());
                String val = new String(bytes);
                value.set(val);
                writer.append(key, value);
            }

            fs.close();
            writer.close();
            IOUtils.closeStream(writer);
            System.out.println("Status : SUCCESSFUL");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("Status : FAILED");
        }
    }

    public static byte[] toByteArrayUsingJava(InputStream is)
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for(int reads = is.read(); reads != -1; reads = is.read())
            baos.write(reads);

        return baos.toByteArray();
    }
}
