import java.util.*;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import javax.print.DocFlavor;

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

        System.out.println("fuck kafka");

        JavaDStream<OrderWithID> orderDStream = stream.map(
          new Function<ConsumerRecord<String, String>, OrderWithID>() {
              @Override
              public OrderWithID call(ConsumerRecord<String, String> cr) throws Exception {
                  System.out.println(cr.key());
                  System.out.println(cr.value());
                  return JSONObject.parseObject(cr.value(), OrderWithID.class);
              }
          }
        ).cache();
        orderDStream.print();

        System.out.println("stream map ok");
        orderDStream.foreachRDD(
                new VoidFunction<JavaRDD<OrderWithID>> () {
                    @Override
                    public void call(JavaRDD<OrderWithID> rdd) throws Exception {
                        List<OrderWithID> orders = rdd.collect();
                        for (OrderWithID o : orders) {
                            System.out.println("in List");
                            System.out.println(o.getOrder_id());
                            System.out.println(o.getOrder().getInitiator());
                        }
                        /*
                        rdd.foreach(
                                new VoidFunction<OrderWithID>() {
                                    @Override
                                    public void call(OrderWithID orderWithID) throws Exception {
                                        System.out.println("order with ID:");
                                        System.out.println(JSON.toJSONString(orderWithID));
                                    }
                                }
                        );
                        */
                    }
                }
        );

/*
        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        wordCounts.toJavaDStream().foreachRDD(rdd -> {
            System.out.println("hhhhhh");
            System.out.println("rdd: " + rdd.toString());
        });
*/
        System.out.println("fuck kafka");

        //stream.print();

        sparkContext.start();
        sparkContext.awaitTermination();

    }

    private void handleOrder(OrderWithID o) {

    }


}
