
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_2 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text lineText, Context context) throws IOException, InterruptedException {
			
			try {
			Configuration conf = context.getConfiguration();
			
			String line = lineText.toString();
			if(line.split(",")[0].equals("date")) {
				
			}
			else {
			String country = line.split(",")[1];
			String deaths = line.split(",")[3];
			
			SimpleDateFormat dateFormatter=new SimpleDateFormat("yyyy-MM-dd");
			
			String dateString = line.split(",")[0];
			Date currentDate = dateFormatter.parse(dateString);
			Timestamp currentTs=new Timestamp(currentDate.getTime());  
			
			String startDateString = conf.get("startDate");
			Date startDate = dateFormatter.parse(startDateString);
			Timestamp startDateTs = new Timestamp(startDate.getTime());
			
			String endDateString = conf.get("endDate");
			Date endDate = dateFormatter.parse(endDateString);
			Timestamp endDateTs = new Timestamp(endDate.getTime());

			if((currentTs.after(startDateTs) && currentTs.before(endDateTs)) || currentTs.equals(startDateTs) || currentTs.equals(endDateTs)){
				context.write(new Text(country), new LongWritable(Integer.parseInt(deaths)));
			}
			//StringTokenizer tok = new StringTokenizer(value.toString(), ";\"\'. \tâ€˜â€œ"); 
			else{
			}
			}
		}
		catch(ParseException e) {
			System.out.println(e);
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
		conf.set("startDate", args[1]);
		conf.set("endDate", args[2]);
		try {
		SimpleDateFormat dateFormatter=new SimpleDateFormat("yyyy-MM-dd");
		Date startDate = dateFormatter.parse(args[1]);
		Timestamp startDateTs = new Timestamp(startDate.getTime());
		
		Date endDate = dateFormatter.parse(args[2]);
		Timestamp endDateTs = new Timestamp(endDate.getTime());
		
		Date dataStartDate = dateFormatter.parse("2019-12-30");
		Timestamp dataStartDateTs = new Timestamp(dataStartDate.getTime());
		
		Date dataEndDate = dateFormatter.parse("2020-04-09");
		Timestamp dataEndDateTs = new Timestamp(dataEndDate.getTime());
		
		if(endDateTs.before(startDateTs)) {
			System.out.println("ERROR! End Date is before Start Date. "
					+ "Please enter Start Date prior to End Date and try again!");	
		}
		else if(!(startDateTs.after(dataStartDateTs) && startDateTs.before(dataEndDateTs)
				&& endDateTs.after(dataStartDateTs) && endDateTs.before(dataEndDateTs))) {
			System.out.println("ERROR! Dates are not within the given data date ranges. "
					+ "Please enter the dates in between the range of  2019-12-31 and 2020-04-08 and try again!");
		}
		else {
		Job myjob = Job.getInstance(conf, "Covid19 count cases in date range");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		}
		}
		catch(ParseException pe){
			System.out.println("ERROR! Please enter start date and/or end date in (YYYY-MM-dd) date format and try again!"
					+ "and try again! ");
		}
	}
}
