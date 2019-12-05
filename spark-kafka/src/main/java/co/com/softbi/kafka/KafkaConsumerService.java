package co.com.softbi.kafka;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class KafkaConsumerService {

	public static void main(String[] args) throws InterruptedException {
		try {
			Logger.getLogger("org").setLevel(Level.OFF);
			Logger.getLogger("akka").setLevel(Level.OFF);
			
			Map<String, Object> kafkaParams = new HashMap<String, Object>();
			kafkaParams.put("bootstrap.servers", "localhost:9092");
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
			kafkaParams.put("auto.offset.reset", "latest");
			kafkaParams.put("enable.auto.commit", false);

			Collection<String> topics = Arrays.asList("bvc");

			SparkConf sparkConf = new SparkConf();
			sparkConf.setMaster("local[*]");
			sparkConf.setAppName("WordCountingApp");

			JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

			JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

			JavaPairDStream<String, String> results = messages
					.mapToPair(record -> new Tuple2<String, String>(record.key(), record.value()));

			JavaDStream<String> lines = results.map(tuple2 -> tuple2._2());
			/* Se imprime el valor del mensaje */
			lines.foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" :: "+ x)));			

			streamingContext.start();
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {			
			 e.printStackTrace();
		}
	}
}
