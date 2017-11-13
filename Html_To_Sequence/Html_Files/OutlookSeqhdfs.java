package OutlookHTMLcleansing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class OutlookSeqhdfs {
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		int mb = 1024*1024;
        
        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
        
        
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		Path inputFile = new Path(args[1]);
		Path outputFile = new Path(args[2]);

		FSDataInputStream inputStream;
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outputFile, key.getClass(), value.getClass());
		

		
			FileStatus[] fStatus = fs.listStatus(inputFile);

			for (FileStatus fst : fStatus)
			{

				String str = "";
				System.out.println("Processing file : " + fst.getPath().getName() + " and the size is : " + fst.getPath().getName().length());

				inputStream = fs.open(fst.getPath());
				byte[] bytes = toByteArrayUsingJava(inputStream);
				key.set(fst.getPath().getName().toString());
				String val = new String(bytes);
				value.set(val);

				writer.append(key, value);

			}
		
		fs.close();
		writer.close();
		IOUtils.closeStream(writer);
		System.out.println("SEQUENCE FILE CREATED SUCCESSFULLY........");
	}

	/**
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static byte[] toByteArrayUsingJava(InputStream is) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int reads = is.read();

		while (reads != -1) {
			baos.write(reads);
			reads = is.read();
		}

		return baos.toByteArray();

	}


}
