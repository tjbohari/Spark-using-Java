import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
	import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
	import org.apache.spark.sql.Row;
	import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
public class Logistic_Regression {


	// Load training data
	Dataset<Row> training = spark.read().format("libsvm")
	  .load("data/mllib/sample_libsvm_data.txt");

	LogisticRegression lr = new LogisticRegression()
	  .setMaxIter(10)
	  .setRegParam(0.3)
	  .setElasticNetParam(0.8);

	// Fit the model
	LogisticRegressionModel lrModel = lr.fit(training);

	// Print the coefficients and intercept for logistic regression
	System.out.println("Coefficients: "
	  + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

	// We can also use the multinomial family for binary classification
	LogisticRegression mlr = new LogisticRegression()
	        .setMaxIter(10)
	        .setRegParam(0.3)
	        .setElasticNetParam(0.8)
	        .setFamily("multinomial");

	// Fit the model
	LogisticRegressionModel mlrModel = mlr.fit(training);

	// Print the coefficients and intercepts for logistic regression with multinomial family
	System.out.println("Multinomial coefficients: " + lrModel.coefficientMatrix()
	  + "\nMultinomial intercepts: " + mlrModel.interceptVector());
}




//program from slides

public static void main(String[] args) {
SparkConf sparkConf = new SparkConf().setAppName("JavaBookExample");
JavaSparkContext sc = new JavaSparkContext(sparkConf);
//Load 2 types of emails from text files: spam and ham (non-spam).
//Each line has text from one email:
JavaRDD<String> spam = sc.textFile("files/spam.txt"); // RDD of lines of (spam) text from different emails
JavaRDD<String> ham = sc.textFile("files/ham.txt");
//Create a HashingTF instance to map email text to vectors of 100 features:
final HashingTF tf = new HashingTF(100);

//Each email is split into words, and each word is mapped to one feature.
//Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
JavaRDD<LabeledPoint> positiveExamples = spam.map(email -> new LabeledPoint(1/*spam*/, tf.transform(Arrays.asList(email.split(" ")))));
JavaRDD<LabeledPoint> negativeExamples = ham.map(email -> new LabeledPoint(0/*not spam*/, tf.transform(Arrays.asList(email.split(" ")))));
JavaRDD<LabeledPoint> trainingData = positiveExamples.union(negativeExamples);
trainingData.cache(); // Caches data since Logistic Regression is an iterative algorithm (training data needs to be revisited many times).

//Create a (binary) Logistic Regression learner:
LogisticRegressionWithSGD lrLearner = new LogisticRegressionWithSGD();
//Run the actual learning algorithm on the training data.
LogisticRegressionModel model = lrLearner.run(trainingData.rdd());
//Test on a positive example (spam) and a negative one (ham).
//First apply the same HashingTF feature transformation used on the training data.
Vector posTestExample =
tf.transform(Arrays.asList("O M G unbeliiievable bargain buy!!!!".split(" ")));
Vector negTestExample =
tf.transform(Arrays.asList("Hi Mom, yesterday I started studying Spark.".split(" ")));
//Now use the learned model to predict spam/ham for new emails.
System.out.println("Prediction for positive test example: " + model.predict(posTestExample));
System.out.println("Prediction for negative test example: " + model.predict(negTestExample));
sc.stop();
}









public static void main(String[] args) throws Exception { 

SparkConf sparkConf = new SparkConf().setAppName("BookClassification"); 

JavaSparkContext sc = new JavaSparkContext(sparkConf); 

JavaRDD<String> labelledAbstracts = sc.textFile("abstractsLabelled.txt"); 

HashingTF tf = new HashingTF(100); 

JavaRDD<LabeledPoint> allData = labelledAbstracts.map(
		(String s) -> {
			
			String[] abst = new String[s.length()];
			for (int i =0; i < abst.length; i++) {
				abst[i] = s.split(" ");
			}
			if(abst[abst.length - 1]=="S") {
				label=0;
				}
				if(tokens[abst.length - 1]=="N") {
				label=1;
				}
			String text = new String[abst.length]
			for (int i = 0; i < abst.length -1; i++) {
				text[i] = abst[i];
			}
			Tuple2<String, Vector> data_features =  new Tuple2<String, Vector>(abst[abst.length - 1], Vectors.dense(text));
			return new LabeledPoint(abst[abst.length - 1], tf.transform(Vectors.dense(text))); 
		}
		);

JavaRDD<LabeledPoint> trainingData = allData.parsedData.sample(false, 0.7, 50L); 

SVMModel model = SVMWithSGD.train(trainingData.rdd(), 100);

Vector test = tf.transform(Vectors.dense("This is a random text to test SVM model"));


System.out.println("Prediction: " + model.predict(test)); 

sc.stop(); sc.close(); 

} 
