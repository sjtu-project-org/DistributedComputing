import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class JavaDirectDemo {

    private static final Pattern SPACE = Pattern.compile(" ");

    private static final String topic = "test1";

    public static void main(String[] args) throws Exception{

        SparkConf conf = new SparkConf()
                .setMaster("spark://server-1:7077")
                .setAppName("JavaDirectDemo");
        JavaStreamingContext sparkContext = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.0.0.52:9092, 10.0.0.34:9092, 10.0.0.101:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "group-demo2");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", "true");

        Collection<String> topics = Arrays.asList(topic);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        sparkContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.toJavaDStream().foreachRDD(rdd -> {
            System.out.println("hhhhhh");
            System.out.println("rdd: " + rdd.toString());
        });

        System.out.println("fuck kafka");

        //stream.print();

        sparkContext.start();
        sparkContext.awaitTermination();

    }
}
