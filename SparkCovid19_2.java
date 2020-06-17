/* Java imports */
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Iterable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
/* Spark imports */
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
public class SparkCovid19_2 {

    
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Output file path on distributed file system
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	System.out.println("Hello World from Java");

	String input = args[0];
	String output = args[2];
	
	String populationsPathString = args[1];
	/* essential to run any spark code */
	SparkConf conf = new SparkConf().setAppName("Spark Covid19 Task 2");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
    JavaRDD<String> data = sc.textFile(populationsPathString);
    JavaRDD<List<String>> populationMapRDD = data.map(new Function<String, List<String>>() {
        @Override
        public List<String> call(String row) throws Exception {
            return createPopulationMapStringList(row);
        }

        private List<String> createPopulationMapStringList(String s) {
        	List<String> populationList = new ArrayList<>();
            if(s.split(",")[1].equals("Jersey") || s.split(",")[1].equals("Guernsey") || s.split(",")[2].equals("Bonaire Sint Eustatius and Saba") || s.split(",")[0].equals("countriesAndTerritories")) {}
            else {
            String country = s.split(",")[1];
            String population = s.split(",")[4];
            populationList.add(country);
            populationList.add(population);
            }
            return populationList;
        }
    });
    
    List<List<String>> populationStringMap = populationMapRDD.collect();
	Map<String, Integer> populationMap = new HashMap<>();
	
	for(List<String> populationString: populationStringMap) {
		if(!populationString.isEmpty()) {
		String country = populationString.get(0);
		int population = Integer.parseInt(populationString.get(1));
		populationMap.put(country, population);
		}
	}
	Broadcast<Map<String, Integer>> populationMapBC = sc.broadcast(populationMap);
	/* load input data to RDD */
	JavaRDD<String> dataRDD = sc.textFile(args[0]);

	JavaPairRDD<String, Double> counts =
	    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
		    public Iterator<Tuple2<String, Double>> call(String value){
			String date = value.split(",")[0];
			List<Tuple2<String, Double>> retWords = new ArrayList<Tuple2<String, Double>>();
			if(value.split(",")[0].equals("date") || value.split(",")[1].equals("Guernsey") || value.split(",")[1].equals("Bonaire Sint Eustatius and Saba") || value.split(",")[1].equals("Jersey")) {}
			else {
				if(populationMapBC.value().get(value.split(",")[1]) != null) {
				int population = populationMapBC.value().get(value.split(",")[1]);
				double perMillion = (Double.parseDouble(value.split(",")[2])/population)*1000000;
			    retWords.add(new Tuple2<String, Double>(value.split(",")[1], perMillion));			
			}
		}
			return retWords.iterator();
		}}).reduceByKey(new Function2<Double, Double, Double>(){
			public Double call(Double x, Double y){
			    return x+y;
			}
		    });
	
	counts.saveAsTextFile(output);
	
    }
}
