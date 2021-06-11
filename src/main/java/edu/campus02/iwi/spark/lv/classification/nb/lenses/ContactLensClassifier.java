package edu.campus02.iwi.spark.lv.classification.nb.lenses;

import java.util.Arrays;

import edu.campus02.iwi.spark.lv.WinConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ContactLensClassifier {

	public static void main(String[] args) {

		WinConfig.setupEnv();

		// TODO: 1) set spark config and init spark session
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName(ContactLensClassifier.class.getName());
		// TODO: 2) read raw data as csv with corresponding options
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		System.out.println("Working Directory = " + System.getProperty("user.dir"));
		Dataset<Row> ds = spark.read()
				.option("header", true)
				.option("delimiter",";")
				.option("nullValue", "\\N")
				.option("inferSchema", true)
				.csv("data/input/lv/lenses/contact-lenses.csv").cache();
		ds.printSchema();
		
		// TODO: 3) combine the separate columns as feature vector
		// all f0..f8 => 9 columns in total
		VectorAssembler toVec= new VectorAssembler().setInputCols(Arrays.copyOf(ds.columns(), 9)).setOutputCol("features");
		
		// TODO: 4) transform the raw data
		Dataset<Row> data= toVec.transform(ds);
		
		// TODO: 5) train a NaiveBayes model on the data
		NaiveBayesModel nbm= new NaiveBayes().setSmoothing(1.0).fit(data);
		
		// TODO: 6) predict a single hard-coded feature vector
		Vector demoVec = Vectors.dense(new double[] {1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0});
		System.out.println("demo vector = "+ demoVec);

		double singlePrediction = nbm.predict(demoVec);
		System.out.println("prediction = "+ singlePrediction);
		
		// TODO: 7) for demo only -> make predictions for all training vectors
		Dataset<Row> predictions= nbm.transform(data).selectExpr("label","prediction","IF(label=prediction,'OK','WRONG') AS result");
		predictions.show(25,false);
		
		// TODO: 8) compute accuracy for complete dataset
		MulticlassClassificationEvaluator evaluator=new MulticlassClassificationEvaluator().setMetricName("accuracy");
		double accuracy= evaluator.evaluate(predictions);
		System.out.println("test data accuracy = "+ accuracy);
		
		// TODO: 9) close spark session
		spark.close();

	}

}