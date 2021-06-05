import org.apache.spark.api.java.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.twitter.*;
import org.spark_project.guava.io.Files;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;
import java.util.Arrays;
import scala.Tuple2;


public class A5Q1 implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
		//setting spark streaming 
		String spark_home = "/home/tjbohari/Spark/sbin";
		String jar = "spark-streaming-twitter_2.11-2.4.0-sources.jar";
		JavaStreamingContext jssc = new JavaStreamingContext("local[2]", "TweetCount", new Duration(1000), spark_home, jar);
		
		jssc.checkpoint(Files.createTempDir().getAbsolutePath());

//		// Setting properties from twitter account to establish connection and
//		// receive the twitter feed
		
		System.setProperty("twitter4j.oauth.consumerKey", "hPtTS0RLV8x7AALtKmljVgRMY");
		System.setProperty("twitter4j.oauth.consumerSecret", "erohz8fpnQiuxtBLdp7SXOB5PCCJoLkCDR8cfXBtqRS72Bn94e");
		System.setProperty("twitter4j.oauth.accessToken", "1334877457699250177-fYOaROMIcFPEhGQVCKxSP7aRnmPYOF");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "vE99byGiNMRkwvwLgTabDhchpDsdEPaQgz5vJM4LHVBld");
		
		//creating stream to get tweets in english
		JavaDStream<Status> tweets =  TwitterUtils.createStream(jssc).filter(status->status.getLang().equals("en"));
		JavaDStream<String> statuses = tweets.map(T -> {return T.getText();});
	
		statuses.print(); //printing tweets recieved
		
		//printing the word count, character count for tweets in a window
		statuses.foreachRDD(f -> {
			JavaRDD<Integer> word_tweet_length = f.map(t -> t.split(" ").length);
      		int word_total_length = word_tweet_length.reduce((a ,b) ->  a+b);
      		JavaRDD<Integer> char_tweet_length = f.map(t -> t.length());
      		int char_total_length = char_tweet_length.reduce((a ,b) ->  a+b);
      		System.out.println("word count  " + word_total_length);
      		System.out.println("character count  " + char_total_length);
		});
		
		//printing the hashtags in the window
		JavaDStream<String> words = statuses.flatMap(in -> {
	         return Arrays.asList(in.split(" ")).iterator();
	         });
		
		JavaDStream<String> hashTags = words.filter(w -> { return w.startsWith("#"); });
		hashTags.print();
		
		//printing average word and character counts for the tweet
		statuses.foreachRDD(f ->{
      		long tweet_count = f.count();
      		JavaRDD<Integer> word_tweet_length = f.map(t -> t.split(" ").length);
      		int word_total_length = word_tweet_length.reduce((a ,b) ->  a+b);
      		JavaRDD<Integer> char_tweet_length = f.map(t -> t.length());
      		int char_total_length = char_tweet_length.reduce((a ,b) ->  a+b);
      		System.out.println("average word count  " + (double)word_total_length / tweet_count);
      		System.out.println("average character count  " + (double)char_total_length / tweet_count);
		});
		
		//printing top 10 hashtags in the tweets
		JavaPairDStream<String, Integer> pairs = hashTags.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        //reducing the pairs for counting the hashtags
		JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey((v1, v2) -> v1 + v2);
		//swapping for sorting on basis of keys 
        JavaPairDStream<Integer, String> s_wordcount = wordCount.mapToPair(x -> x.swap());
        JavaPairDStream<Integer, String> sorted = s_wordcount.transformToPair(in -> in.sortByKey(false));
        JavaPairDStream<String, Integer> swap_again = sorted.mapToPair(x -> x.swap()); //swapping again for arranging the results 
        swap_again.print(10); //printing first 10 results
        
		//setting the window for 30 seconds and running it for 5 minutes
		JavaDStream<String> tweet_window = statuses.window(Durations.minutes(5), Durations.seconds(30));
		
		tweet_window.foreachRDD(f ->{
      		long tweet_count = f.count();
      		JavaRDD<Integer> word_tweet_length = f.map(t -> t.split(" ").length);
      		int word_total_length = word_tweet_length.reduce((a ,b) ->  a+b);
      		JavaRDD<Integer> char_tweet_length = f.map(t -> t.length());
      		int char_total_length = char_tweet_length.reduce((a ,b) ->  a+b);
      		System.out.println("printing inside window");
      		System.out.println("average word count  " + (double)word_total_length / tweet_count);
      		System.out.println("average character count  " + (double)char_total_length / tweet_count);
		});
		
		//using the window of 30 seconds to get each word for hastag filter
		JavaDStream<String> words_w = tweet_window.flatMap(in -> {
	         return Arrays.asList(in.split(" ")).iterator();
	         });
		
		JavaDStream<String> hashtags_w = words_w.filter(w -> { return w.startsWith("#"); });
		
		//creating pairs of each hashtags for count
		JavaPairDStream<String, Integer> pairs_window = hashtags_w.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        //reducing the pairs for counting the hashtags
		JavaPairDStream<String, Integer> wordCount_window = pairs_window.reduceByKey((v1, v2) -> v1 + v2);
		//swapping for sorting on basis of keys 
        JavaPairDStream<Integer, String> s_wordcount_window = wordCount_window.mapToPair(x -> x.swap());
        JavaPairDStream<Integer, String> sorted_window = s_wordcount_window.transformToPair(in -> in.sortByKey(false));
        JavaPairDStream<String, Integer> swap_again_window = sorted_window.mapToPair(x -> x.swap()); //swapping again for arranging the results 
        swap_again_window.print(10); //printing first 10 results
        
		//starting the streaming context
        try{
			jssc.start();
			jssc.awaitTermination();
		}
		catch(Exception E) {};
		
	}

}