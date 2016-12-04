/* SimpleApp.java */
import org.apache.spark.api.java.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class Indexer {

	private class DocInfo{

		int docId;
		int word_count; 
		double tf_score;
		double idf_score;

		DocInfo(int dId, int count, double tf, double idf){
			docId = dId;
			word_count= count;
			tf_score = tf;
			idf_score = idf;
		}

		public int getDocId() {
			return docId;
		}

		public void setDocId(int docId) {
			this.docId = docId;
		}

		public int getWord_count() {
			return word_count;
		}

		public void setWord_count(int word_count) {
			this.word_count = word_count;
		}

		public double getTf_score() {
			return tf_score;
		}

		public void setTf_score(double tf_score) {
			this.tf_score = tf_score;
		}

		public double getIdf_score() {
			return idf_score;
		}

		public void setIdf_score(double idf_score) {
			this.idf_score = idf_score;
		}

	}

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String sourceDir = "/home/cis555/workspace/spark-app/files/";


		HashMap<String, Integer> docIdMap = new HashMap<String, Integer>();
		HashMap<String, DocInfo> invertedIndex = new HashMap<String, DocInfo>();

		JavaPairRDD<String, Integer> wordtuples = null ;

		SparkSession spark = SparkSession
				.builder()
				.appName("JavaWordCount")
				.getOrCreate();


		int id = 1;

		JavaPairRDD<String,String> files = sc.wholeTextFiles(sourceDir).cache();

		// associate the document with a document id. HERE?	

		//docIdMap.put(f.getName(), new Integer(id));
		JavaRDD<String> bodies = files.values();
		
		
		//JavaRDD<Integer, String, Double, Double> index_element;
		//JavaRDD<Tuple4<String,String, String, String>> index;
		
		
		JavaRDD<String> words = bodies.flatMap(new FlatMapFunction<String, String>() {
			//@Override
			public Iterator<String> call(String s) {
				return Arrays.asList(SPACE.split(s)).iterator();
			}
		});
		

		JavaPairRDD<String, Integer> ones = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					//@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				});



		JavaPairRDD<String, Integer> counts = ones.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					//@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		JavaRDD<String> file_names = files.keys();
		List<String> names = file_names.collect();
		for(String name : names  ){
			System.out.println("filename: " + name);
		}

		spark.stop();
		//sc.stop();
	}
}








