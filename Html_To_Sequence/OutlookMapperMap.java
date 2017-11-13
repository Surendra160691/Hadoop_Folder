// Decompiled by DJ v3.12.12.101 Copyright 2016 Atanas Neshkov  Date: 11/13/2017 9:47:21 AM
// Home Page:  http://www.neshkov.com/dj.html - Check often for new version!
// Decompiler options: packimports(3) 
// Source File Name:   OutlookMapper.java

package com.hp.it.cx;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// Referenced classes of package com.hp.it.cx:
//            OutlookPatternextract, OutlookMapper

public class OutlookMapperMap extends Mapper<Object , Object, Object , Object >
{

    public void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
    {
        mos = new MultipleOutputs(context);
        emailCategory = context.getConfiguration().get("EmailCategory");
    }

    public void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
    {
        String title = "<title>(.+?)</title>";
        String messid = "<!--X-Message-Id:(.+?)-->";
        String date = "<li><em>Date</em>:(.+?)</li>";
        String from = "<li><em>From</em>.+?<a.+?>(.+?)</a>.*?</li>";
        String bodycon = "<!--X-Body-of-Message-->([\\w\\W]*?)<!--X-Body-of-Message-End-->";
        String outputdata = "";
        String titlecontent = OutlookPatternextract.extract(value.toString(), title);
        titlecontent = titlecontent.trim();
        if(titlecontent.endsWith("\\"))
            titlecontent = titlecontent.substring(0, titlecontent.length() - 1);
        String datecontent = OutlookPatternextract.extract(value.toString(), date);
        String fromcontent = OutlookPatternextract.extract(value.toString(), from);
        String bodycontent = OutlookPatternextract.extract(value.toString(), bodycon);
        bodycontent = bodycontent.trim();
        if(bodycontent.endsWith("\\"))
            bodycontent = bodycontent.substring(0, bodycontent.length() - 1);
        bodycontent = bodycontent.replaceAll("\034", "");
        bodycontent = bodycontent.replaceAll("\037", "");
        String id = OutlookPatternextract.extract(value.toString(), messid);
        String stripped_body = bodycontent.replaceAll("<[\\w\\W]*?>", "");
        stripped_body = stripped_body.replaceAll("&nbsp;", "");
        stripped_body = stripped_body.replaceAll("\n", "");
        stripped_body = stripped_body.replaceAll("&bull;", " * ");
        stripped_body = stripped_body.replaceAll("&lsaquo;", "<");
        stripped_body = stripped_body.replaceAll("&rsaquo;", ">");
        stripped_body = stripped_body.replaceAll("&trade;", "(tm)");
        stripped_body = stripped_body.replaceAll("&frasl;", "/");
        stripped_body = stripped_body.replaceAll("&lt;", "<");
        stripped_body = stripped_body.replaceAll("&gt;", ">");
        stripped_body = stripped_body.replaceAll("&copy;", "(c)");
        stripped_body = stripped_body.replaceAll("&reg;", "(r)");
        stripped_body = stripped_body.trim();
        if(stripped_body.endsWith("\\"))
            stripped_body = stripped_body.substring(0, stripped_body.length() - 1);
        String datetimestamp = datecontent;
        if(id != null && !id.trim().isEmpty())
        {
            if(datecontent.contains("+") || datecontent.contains("-"))
            {
                int position = datecontent.length();
                if(datecontent.contains("+"))
                    position = datecontent.indexOf('+');
                if(datecontent.contains("-"))
                    position = datecontent.indexOf('-');
                datetimestamp = datecontent.substring(0, position);
            }
            outputdata = (new StringBuilder(String.valueOf(id.trim()))).append("\034").append(key.toString()).append("\034").append(datetimestamp.trim()).append("\034").append(datecontent.trim()).append("\034").append(fromcontent.trim()).append("\034").append(titlecontent.trim()).append("\034").append(bodycontent.trim()).append("\034").append(stripped_body).append("\034").append(emailCategory).append("\037").toString();
            mos.write("OutlookMail", new Text(outputdata), NullWritable.get());
        } else
        {
            mos.write("ErrorOutlookMail", new Text(key), NullWritable.get());
        }
    }

    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
    {
        mos.close();
    }

    public void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
    {
        map((Text)obj, (Text)obj1, (org.apache.hadoop.mapreduce.Mapper.Context)context);
    }

    private MultipleOutputs mos;
    private String emailCategory;

    public OutlookMapperMap()
    {
        mos = null;
        emailCategory = null;
    }
}
