import java.util.*;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import scala.Tuple2;

import javax.print.DocFlavor;

import Lock.FairLock;

public class JavaDirectDemo {

    private static final Pattern SPACE = Pattern.compile(" ");

    private static final String topic = "test1";

    private static SparkConf sparkConf;
    private static JavaStreamingContext sparkContext;

    private static ZooKeeper zkClient;
    private static final String interHost = "10.0.0.52:2181";

    private static final Integer commNum = 4;
    private static final String[] commList = new String[]{"1", "2", "3", "4"};
    private static Map<String, FairLock> itemLock = new HashMap<>();

    // mysql property
    private static JavaSparkContext sparkContextForSQL;
    private static SQLContext sqlContext;
    private static Properties mysqlProp = new Properties();
    private static String mysqlUrl = "jdbc:mysql://10.0.0.37:3306/dslab";
    //表名
    private static final String resultTable = "result";
    private static final String commTable = "commodity";



    public static void main(String[] args) throws Exception{

        // 创建 ZK 用于 total amount
        zkClient = new ZooKeeper(interHost, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState()== Event.KeeperState.Disconnected){
                    System.out.println("HTTP Receiver lost ZK connection");
                }
            }
        });
        // 创建各个商品的 lockServer
        for (int i=0; i<commNum; i++) {
            itemLock.put(commList[i], new FairLock(commList[i]));
        }

        // 初始化 mysql 属性
        mysqlProp.put("user","root");
        mysqlProp.put("password","123");
        mysqlProp.put("driver","com.mysql.jdbc.Driver");

        // 实例化 spark config 和 spark context
        sparkConf = new SparkConf()
                .setMaster("spark://server-1:7077")
                .setAppName("JavaDirectDemo");
        sparkContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 实例化 sqlContext
        /*
        sparkContextForSQL = new JavaSparkContext(new SparkConf()
                .setAppName("SparkMysql")
                .setMaster("spark://server-1:7077"));*/
        sparkContextForSQL = sparkContext.sparkContext(); // JavaStreamingContext 是对 JavaSparkContext 的封装
        sqlContext = new SQLContext(sparkContextForSQL);

        // 初始化 Kafka 属性
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
                            handleOrder(o);
                            //System.out.println(o.getOrder_id());
                            //System.out.println(o.getOrder().getInitiator());
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

    private static void handleOrder(OrderWithID o) throws Exception {


        List resultStructFields = new ArrayList();
        resultStructFields.add(DataTypes.createStructField("id",DataTypes.StringType,true));
        resultStructFields.add(DataTypes.createStructField("user_id",DataTypes.StringType,true));
        resultStructFields.add(DataTypes.createStructField("initiator",DataTypes.StringType,true));
        resultStructFields.add(DataTypes.createStructField("success",DataTypes.StringType,true));
        resultStructFields.add(DataTypes.createStructField("paid",DataTypes.DoubleType,true));

        List commStructFields = new ArrayList();
        commStructFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        commStructFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        commStructFields.add(DataTypes.createStructField("price",DataTypes.DoubleType,true));
        commStructFields.add(DataTypes.createStructField("currency",DataTypes.StringType,true));
        commStructFields.add(DataTypes.createStructField("inventory",DataTypes.IntegerType,true));

        // get items need to lock
        List<Item> itemList = o.getOrder().getItems();
        Collections.sort(itemList, new Comparator<Item>(){
            @Override
            public int compare(Item i1, Item i2) {
                return i1.getId().compareTo(i2.getId());
            }
        });

        Boolean stockEnough = true;
        List<Row> commCache = new ArrayList<>();
        // acquire item locks
        // get inventory from DB and judge
        for (Item item : itemList) {
            itemLock.get(item.getId()).acquireLock();

            Dataset<Row> DF = sqlContext
                    .read().jdbc(mysqlUrl,commTable,mysqlProp)
                    .select("*").where("id = "+ item.getId());

            Integer invent = DF.first().getInt(4);
            if (invent < item.getNumber()) {
                stockEnough = false;
                break;
            }
            commCache.add(DF.first());
        }

        // 计算 paid 并构造数据写入MySQL
        Double paid = 0.0;
        if (stockEnough) {
            paid = 55.0;
        }
        String result = String.format("%s %s %s %s %s",
                o.getOrder_id(),
                o.getOrder().getUserId(),
                o.getOrder().getInitiator(),
                "false",
                String.valueOf(paid));
        JavaRDD<String> resultData = sparkContextForSQL.parallelize(Arrays.asList(result));
        JavaRDD<Row> resultRDD = resultData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                //return RowFactory.create(Integer.valueOf(splited[0]),splited[1],Integer.valueOf(splited[2]));
                return RowFactory.create(
                        splited[0],
                        splited[1],
                        splited[2],
                        splited[3],
                        Double.valueOf(splited[4])
                );
            }
        });
        StructType resultStruct = DataTypes.createStructType(resultStructFields);
        Dataset<Row> resultDF = sqlContext.createDataFrame(resultRDD, resultStruct);
        resultDF.write().mode("append").jdbc(mysqlUrl,resultTable,mysqlProp);

        // 更新库存
        // release item locks
        for (int i=0; i<itemList.size(); i++) {
            Item item = itemList.get(i);
            Row comm = commCache.get(i);
            if (stockEnough) {
                String newComm = String.format("%s %s %s %s %s",
                        comm.getString(0),
                        comm.getString(1),
                        comm.getString(2),
                        comm.getString(3),
                        comm.getInt(4) - item.getNumber());

                JavaRDD<String> itemData = sparkContextForSQL.parallelize(Arrays.asList(newComm));
                JavaRDD<Row> itemRDD = itemData.map(new Function<String,Row>(){
                    public Row call(String line) throws Exception {
                        String[] splited = line.split(" ");
                        //return RowFactory.create(Integer.valueOf(splited[0]),splited[1],Integer.valueOf(splited[2]));
                        return RowFactory.create(
                                splited[0],
                                splited[1],
                                splited[2],
                                splited[3],
                                Double.valueOf(splited[4])
                        );
                    }
                });
                StructType itemStruct = DataTypes.createStructType(commStructFields);
                Dataset<Row> itemDF = sqlContext.createDataFrame(itemRDD, itemStruct);
                itemDF.write().mode("overwrite").jdbc(mysqlUrl,commTable,mysqlProp);
            }

            itemLock.get(item.getId()).releaseLock();
        }
    }


}
