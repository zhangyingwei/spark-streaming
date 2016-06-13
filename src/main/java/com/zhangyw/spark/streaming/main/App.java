package com.zhangyw.spark.streaming.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class App {
	public static void main(String[] args) {
		String zkQuorum = "192.168.58.130:2181";
		String topicss = "test";
		int numThread = 2;

		Duration batchInterval = new Duration(5000);
		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
//		sparkConf.setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,new Duration(2000));
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topicss, numThread);
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, null, topicMap);
		messages.print();
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
