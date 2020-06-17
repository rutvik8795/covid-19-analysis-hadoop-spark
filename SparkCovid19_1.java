/* Java imports */
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
/* Spark imports */
import scala.Tuple2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

public class SparkCovid19_1 {

    
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Output file path on distributed file system
     */
    public static void main(String[] args){
	System.out.println("Hello World from Java");

	String input = args[0];
	String output = args[3];

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
			
			/* essential to run any spark code */
			SparkConf conf = new SparkConf().setAppName("Spark Covid19 Task 1");
			JavaSparkContext sc = new JavaSparkContext(conf);
			Broadcast<String> startDateStringBC = sc.broadcast(args[1]);
			Broadcast<String> endDateStringBC = sc.broadcast(args[2]);
			/* load input data to RDD */
			JavaRDD<String> dataRDD = sc.textFile(args[0]);

			JavaPairRDD<String, Integer> counts =
			    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
				    public Iterator<Tuple2<String, Integer>> call(String value) throws ParseException{
				    List<Tuple2<String, Integer>> retWords = new ArrayList<Tuple2<String, Integer>>();
					if(value.split(",")[0].equals("date")) {}
					else {
				    SimpleDateFormat dateFormatter=new SimpleDateFormat("yyyy-MM-dd");
				    
					String dateString = value.split(",")[0];
					Date currentDate = dateFormatter.parse(dateString);
					Timestamp currentTs=new Timestamp(currentDate.getTime());  
					
					String startDateString = startDateStringBC.value();
					Date startDate = dateFormatter.parse(startDateString);
					Timestamp startDateTs = new Timestamp(startDate.getTime());
					
					String endDateString = endDateStringBC.value();
					Date endDate = dateFormatter.parse(endDateString);
					Timestamp endDateTs = new Timestamp(endDate.getTime());
			
					if((currentTs.after(startDateTs) && currentTs.before(endDateTs)) || currentTs.equals(startDateTs) || currentTs.equals(endDateTs)){
						//context.write(new Text(country), new LongWritable(Integer.parseInt(deaths)));
						retWords.add(new Tuple2<String, Integer>(value.split(",")[1],Integer.parseInt(value.split(",")[3]) ));
					}
					}
					return retWords.iterator();
				    }
				}).reduceByKey(new Function2<Integer, Integer, Integer>(){
					public Integer call(Integer x, Integer y){
					    return x+y;
					}
				    });
			
			counts.saveAsTextFile(output);
		}
		}
		catch(ParseException pe){
			System.out.println("ERROR! Please enter start date and/or end date in (YYYY-MM-dd) date format and try again!"
					+ "and try again! ");
		}
    }
}
