package edu.campus02.iwi.demo.wc;

import edu.campus02.iwi.spark.lv.WinConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class WordCountDS {

	public static void main(String[] args) {

		WinConfig.setupEnv();

		SparkConf cnf = new SparkConf().setMaster("local[1]")
				.setAppName(WordCountDS.class.getName());
		
		SparkSession spark = SparkSession.builder()
									.config(cnf)
									.getOrCreate();

		Dataset<String> lines = spark.read().text("data/input/demo/les_miserables__victor_hugo.txt")
										.as(Encoders.STRING()).cache();
		
		Dataset<String> words = lines.flatMap((FlatMapFunction<String, String>)
					line -> Arrays.asList(line.split("\\s+")).iterator(),
							Encoders.STRING()
				);
									
		Dataset<Row> counts = words.groupBy("value").count()
				.repartition(1)
				.cache();
		
		counts.show(100,false);

		// It's good practice to explicitly stop the context before
		// your program finishes :)
		spark.close();
		
	}

}
