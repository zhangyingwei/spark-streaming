package com.zhangyw.spark.streaming.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class App {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("spark-streaming-kafka");
		conf.setMaster("local");
		conf.set("spark.streaming.receiver.writeAheadLog.enable", "false");
		conf.set("spark.default.parallelism", "8");//应该为cpu总数的整数倍，10个executers*cores=40
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc,new Duration(3000));
//		jssc.checkpoint("");
		
		Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", "192.168.58.130:2181");
        kafkaParams.put("zookeeper.connection.timeout.ms", "10000");
//        kafkaParams.put("fetch.message.max.bytes", "1073741824");
//        kafkaParams.put("auto.offset.reset", "largest");
//        kafkaParams.put("rebalance.max.retries", "10");
        Map topicMap = new HashMap<String, Integer>();
        topicMap.put("test", 2);
        JavaPairDStream<String,byte[]> streams = KafkaUtils.createStream(jssc,String.class,byte[].class,StringDecoder.class,DefaultDecoder.class,kafkaParams,topicMap,StorageLevel.MEMORY_AND_DISK_SER_2());
//        JavaDStream<String> jdstream = 
        streams.foreachRDD(new Function2() {
			public Object call(Object v1, Object v2) throws Exception {
				System.out.println(v1.toString());
				System.out.println(v2.toString());
				return null;
			}
		});
        jssc.start();
        jssc.awaitTermination();
	}
}
