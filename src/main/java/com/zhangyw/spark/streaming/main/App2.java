package com.zhangyw.spark.streaming.main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class App2 {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
//		if (args.length == 0) {
//			System.err.println("Usage: SparkStreamingFromFlumeToHBaseWindowingExample {master} {host} {port} {table} {columnFamily} {windowInSeconds} {slideInSeconds");
//			System.exit(1);
//		}

		// String master = args[0];
		// String host = args[1];
		// int port = Integer.parseInt(args[2]);
		// int windowInSeconds = 3;// Integer.parseInt(args[5]);
		// int slideInSeconds = 1;// Integer.parseInt(args[5]);

		String zkQuorum = "192.168.58.130";
//		String group = "test-consumer-group";
		String topicss = "test";
		String numThread = "2";

		Duration batchInterval = new Duration(5000);
		// Duration windowInterval = new Duration(windowInSeconds * 1000);
		// Duration slideInterval = new Duration(slideInSeconds * 1000);

		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		sparkConf.setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,new Duration(2000));
		// JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream(host,
		// port);
//		jssc.checkpoint("/user/soft");
		int numThreads = Integer.parseInt(numThread);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = topicss.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, null, topicMap);

		messages.saveAsHadoopFiles("/test/sparktest/", "1.txt",String.class,String.class,TextOutputFormat.class);
//		System.out.println("#############################");
//		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//					public String call(Tuple2<String, String> tuple2) {
//						System.out.println("@@:"+tuple2._2());
//						return tuple2._2();
//					}
//				});
//		System.out.println("#############################");
//		lines.print();
		jssc.start();
		jssc.awaitTermination();
	}
}