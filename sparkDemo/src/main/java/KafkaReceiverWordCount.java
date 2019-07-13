import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 基于Kafka receiver方式的实时wordcount程序
 * @author Administrator
 *
 */
public class KafkaReceiverWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaReceiverWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("helloworld", 1);

        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jssc,
                //"172.20.10.117:2181,172.20.10.118:2181,172.20.10.119:2181",
                "10.0.0.52:2181, 10.0.0.34:2181, 10.0.0.101:2181",
                "DefaultConsumerGroup",
                topicThreadMap);

        lines.print();

        /*
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public Iterator<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return stringStringTuple2._2.split(" ");
                    }
                }
        );
        */
/*
        // 然后开发wordcount逻辑
        JavaDStream<String> words = lines.flatMap(

                new FlatMapFunction<Tuple2<String,String>, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<String> call(Tuple2<String, String> tuple)
                            throws Exception {
                        return Arrays.asList(tuple._2.split(" "));
                    }

                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });

        wordCounts.print();
*/
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            // nothing
        }
        jssc.close();
    }
}