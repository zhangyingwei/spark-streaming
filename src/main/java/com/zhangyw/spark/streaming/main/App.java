package com.zhangyw.spark.streaming.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class App {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("ChanCTKafkaExample");
		conf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
		conf.set("spark.default.parallelism", "60");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(10000L));
		jssc.checkpoint("/user/zhangyw/spark-streaming/");
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("test", 2);
		
		String zkQuorum = "192.168.58.130:2181";
		String groupId = "0";
		String timeout = "10000";
		StorageLevel level = StorageLevel.MEMORY_AND_DISK_SER_2();

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("zookeeper.connect", zkQuorum);
		kafkaParams.put("group.id", groupId);
		kafkaParams.put("zookeeper.connection.timeout.ms", timeout);
		kafkaParams.put("fetch.message.max.bytes", "1073741824");
		kafkaParams.put("auto.offset.reset", "largest");
		kafkaParams.put("rebalance.max.retries", "10");
		
		List<JavaPairDStream<String, byte[]>> streams = new ArrayList<JavaPairDStream<String, byte[]>>();
		for (int i = 0; i < 3; i++) {
			streams.add(KafkaUtils.createStream(jssc, String.class,byte[].class, StringDecoder.class, DefaultDecoder.class,kafkaParams, topicMap, level));
		}
		JavaPairDStream<String, byte[]> rawCdrDStream = jssc.union(streams.get(0), streams.subList(1, 3));
		rawCdrDStream.map(new Function<Tuple2<String,byte[]>, String>() {
			public String call(Tuple2<String, byte[]> v1) throws Exception {
				System.out.println(v1);
				return null;
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}

}
