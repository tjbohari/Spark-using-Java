import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class K_Means {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf sparkConf = new SparkConf()
				.setAppName("WS")
				.setMaster("local[4]").set("spark.executor.memory", "1g")
				.set("spark.driver.allowMultipleContexts" , "true");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//reading data
		JavaRDD<String> data = sc.textFile("src/twitter.txt");
		
		JavaRDD<Tuple2<String, Vector>> parsedData = data.map(
		(String s) -> {
			String[] sarray = s.split(",");
			double[] values = new double[2];
			for (int i = 0; i < 2; i++)
			values[i] = Double.parseDouble(sarray[i]);
			return new Tuple2<String, Vector>(sarray[sarray.length - 1], Vectors.dense(values));
		}
		);
		
		//training the model
		int numClusters = 4;
		int numIterations = 100;
		KMeansModel clusters = KMeans.train(parsedData.map(f -> f._2).rdd(), numClusters, numIterations);
		
		//displaying the result
		parsedData.map(f -> {
		return new Tuple2<String, Integer>(f._1, clusters.predict(f._2));
		}).sortBy(f->f._2, true,1).foreach(f -> System.out.println("Tweet " + f._1 + " is in " + f._2()));
		
		
		sc.stop();
		sc.close();
	}
}
