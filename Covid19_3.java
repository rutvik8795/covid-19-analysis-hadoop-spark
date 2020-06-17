
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_3 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			String country = line.split(",")[1];
			String cases = line.split(",")[2];
			if(line.split(",")[0].equals("date")) {}
			else{
				context.write(new Text(country), new LongWritable(Integer.parseInt(cases)));
			}
		}
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {
		private DoubleWritable total = new DoubleWritable();
		Map<String, Integer> populationMap = new HashMap<>();
		BufferedReader reader;
		public void setup(Context context) throws IOException, InterruptedException
		{
		  int population = 0;
		  URI[] cacheFiles = context.getCacheFiles();
		  if (cacheFiles != null && cacheFiles.length > 0){
			  FileSystem fs = FileSystem.get(context.getConfiguration());
		      Path path = new Path(cacheFiles[0].toString());
		      reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		  }
		  String line = reader.readLine();
		  while(line != null) {
			  if(line.split(",")[0].equals("Guernsey") || line.split(",")[0].equals("Jersey")) {}
		      else if(line.split(",")[4].equals("population")) {}
		      else {
		    	 String country = line.split(",")[1];
		      if(line.split(",")[4] == null) {
		    	  population = 0;
		      }
		      else{
		      population = Integer.parseInt(line.split(",")[4]);
		      populationMap.put(country, population);
		      }
		      }
			    line = reader.readLine();
		     }
		  }
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			String line = reader.readLine();
	    	String keyString = key.toString();
			if(keyString.equals("Bonaire Sint Eustatius and Saba")) {}
			else {
			int population = 0;
			if(populationMap.get(keyString) != null) {
				population = populationMap.get(keyString);
			 if(population != 0) {
				 total.set(Math.round((sum/population)*1000000*100.0)/100.0);
			 }
			// This write to the final output
			context.write(key, total);
			}
		}
		}
	}

	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "Covid19 count cases by country");
		myjob.addCacheFile(new Path(args[1]).toUri());
		myjob.setJarByClass(Covid19_3.class);
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
