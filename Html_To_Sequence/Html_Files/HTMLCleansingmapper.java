package OutlookHTMLcleansing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HTMLCleansingmapper {
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {


				
				public void map(LongWritable key, Text value, Context context)
						throws IOException, InterruptedException
				{
					String line=value.toString();
					
					line=line.replaceAll("<[\\w\\W]*?>", "");
				
                    line = line.replaceAll("&bull;", " * ");
                    line = line.replaceAll("&lsaquo;", "<");
                    line = line.replaceAll("&rsaquo;", ">");
                    line = line.replaceAll("&trade;", "(tm)");
                    line = line.replaceAll("&frasl;", "/");
                    line = line.replaceAll("&lt;", "<");
                    line = line.replaceAll("&gt;", ">");
                    line = line.replaceAll("&copy;", "(c)");
                    line = line.replaceAll("&reg;", "(r)");
                    // Remove all others. More can be added, see
                    // http://hotwired.lycos.com/webmonkey/reference/special_characters/
                    line = line.replaceAll("&(.{2,6});", "");


					
					context.write(new Text(line),NullWritable.get());
				}

	}
}
