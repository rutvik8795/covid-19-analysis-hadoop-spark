
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_1 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text lineText, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			String line = lineText.toString();
			if(line.split(",")[0].equals("date")) {
				
			}
			else {
			String country = line.split(",")[1];
			String cases = line.split(",")[2];
			String date = line.split(",")[0];
			String month = date.split("-")[1];
			if((!Boolean.parseBoolean(conf.get("includeWorld")) && (country.equals("International") || country.equals("World"))) || month.equals("12")){
			}
			//StringTokenizer tok = new StringTokenizer(value.toString(), ";\"\'. \t‘“"); 
			else{
			//while (tok.hasMoreTokens()) {
				//word.set(tok.nextToken());
				context.write(new Text(country), new LongWritable(Integer.parseInt(cases)));
			//}
			}
		}
	}
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		conf.set("includeWorld", args[1]);
		Job myjob = Job.getInstance(conf, "Covid19 count cases by country");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
