package cs523.projectStreamer;

import java.io.BufferedOutputStream;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class StreamRun {
	public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException{		
		Map<String,Object> kafkaParams = new HashMap<>();
    	kafkaParams.put("bootstrap.servers", StreamerConstants.BOOTSTRAP_SERVER);
    	kafkaParams.put("key.deserializer", StringDeserializer.class);
    	kafkaParams.put("value.deserializer", StringDeserializer.class);
    	kafkaParams.put("group.id", StreamerConstants.GROUP_ID);
    	kafkaParams.put("auto.offset.reset", StreamerConstants.AUTO_RESET_PARAM);
    	kafkaParams.put("enable.auto.commit", false);
    	
    	SparkConf sparkConf = new SparkConf().setAppName(StreamerConstants.GROUP_ID).setMaster("local[*]");
    	StreamingContext sc =new StreamingContext(sparkConf, new Duration(20001));
    	Collection<String> topics = Arrays.asList(StreamerConstants.GROUP_ID);
    	InputDStream<ConsumerRecord<String,String>> stream = 
    			KafkaUtils.createDirectStream(
    				sc,
        			LocationStrategies.PreferConsistent(),
        			ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams)

    	);
    	JavaInputDStream<ConsumerRecord<String,String>> dStream = new JavaInputDStream<ConsumerRecord<String,String>>(stream, null);
    	
    	JavaPairDStream<String,String> filteredProduct =dStream
    			.map(f->f.key()+" "+f.value())
    			.map(f->Arrays.asList(f.split(StreamerConstants.SPLIT_DILIMETER)))
    			.map(f->Arrays.asList(f.get(0).split("[ ^'\"]")))
    			.map(e->e.stream().map((k)->{
    				return k.replaceAll("[^a-zA-Z0-9]", " ").trim();
    				}).collect(Collectors.toList()))
    			.map(f->{
    				f.removeIf(StopWords.stopWords::contains);
    				f.removeIf(r->r.matches("\\s*"));
    				return f;
    			})
    			.mapToPair(f->{
    				String id = f.remove(0);
    				f.add(String.valueOf(f.size()));
    				return new Tuple2<String,String>(id,f.toString());
    			});
    	
    	filteredProduct.foreachRDD(rdd->{
    		rdd.saveAsTextFile(StreamerConstants.HDFS_URI+StreamerConstants.DIRECTORY_HDFS+"_"+String.valueOf(LocalDateTime.now().getDayOfYear())+String.valueOf(LocalDateTime.now().getHour())+"/output_"+String.valueOf(LocalDateTime.now().getMinute())+String.valueOf(LocalDateTime.now().getSecond()));
    	});
    	sc.start();
    	
    	sc.awaitTermination();
	}
}

