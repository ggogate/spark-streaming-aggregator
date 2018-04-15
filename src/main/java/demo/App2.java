package demo;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.model.Event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class App2 {

	public static void main(String[] args) throws Exception {
		
		Logger logger = Logger.getRootLogger();
		logger.setLevel(Level.ERROR);
		
		System.setProperty("hadoop.home.dir", "c://hadoop//");
		
		SparkConf sparkConf = new SparkConf().setAppName("aggregator").setMaster("local[2]");		
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(3));
				
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "test");
		//kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Collections.singleton("test");
		
		ObjectMapper mapper = new ObjectMapper();
		
		JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
		    streamingContext,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );
		
		// for each <String, String> in the stream, return the 2nd string which is Event json
		JavaDStream<String> eventsJson = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });
		
		// for each Event json, convert it to an Event object and return a List of Events
		JavaDStream<Event> events = eventsJson.flatMap(new FlatMapFunction<String, Event>(){
			@Override
			public Iterator<Event> call(String eventString) throws Exception {
				Event e = mapper.readValue(eventString, Event.class);
				return (Iterator<Event>) Arrays.asList(e).iterator();
			}			
		});
		
		JavaDStream<Event> aggregates = events.reduce(new Function2<Event,Event,Event>() {

			@Override
			public Event call(Event e1, Event e2) throws Exception {
				Event e = new Event();
				e.setEventName("aggregated");
				e.setAmount(e1.getAmount()+e2.getAmount());
				e.setCount(e1.getCount()+e2.getCount());
				return e;
			}
			
		});
		/*aggregates.foreachRDD(new VoidFunction<JavaRDD<Event>>(){
			@Override
			public void call(JavaRDD<Event> arg0) throws Exception {
				arg0.foreach(VoidFunction<Event>(){
					
				});
			}			
		});*/
		
		JavaDStream<String> aggregatesString = aggregates.flatMap(new FlatMapFunction<Event, String>(){
			@Override
			public Iterator<String> call(Event eventObj) throws Exception {
				String s = mapper.writeValueAsString(eventObj);
				return (Iterator<String>) Arrays.asList(s).iterator();
			}			
		});
		
		aggregatesString.print();
		//stream.print();
		
		streamingContext.start();
		streamingContext.awaitTermination();
	}

}
