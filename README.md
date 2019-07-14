# Lab5 

## System environment
1. Host: 4 virtual servers(centos7) with 4 VCPUs and 8GB RAM is set up
   ![Figure 1](https://raw.githubusercontent.com/zztttt/DistributedComputing/master/report/image-20190713144347113.png)
2. Http request sender: （Windows 的系统配置数据 #TODO ）

## Install and configuration process
1. openstack instance配置与SSH免密登陆：  
    在/.ssh/目录下生成config文件：
    ```
   Host centos-ext
   HostName 202.120.40.8
   User centos
   Port 30360
   IdentityFile /home/zzt/.ssh/test3.pem
   
   Host centos-in2
   HostName 10.0.0.34
   User centos
   Port 22
   ProxyCommand ssh centos-ext -W %h:%p
   IdentityFile /home/zzt/.ssh/test3.pe
   
   Host centos-in3
   HostName 10.0.0.101
   User centos
   Port 22
   ProxyCommand ssh centos-ext -W %h:%p
   IdentityFile /home/zzt/.ssh/test3.pem
   
   Host centos-in4
   HostName 10.0.0.37
   User centos
   Port 22
   ProxyCommand ssh centos-ext -W %h:%p
   IdentityFile /home/zzt/.ssh/test3.pem
   ```
3.  安装JDK和MySQL：  
    直接通过包管理工具yum在centos上安装即可
4.  部署zookeeper  
    从官网下载压缩包：**zookeeper-3.4.14.tar.gz**
    ```
    tar -xvf zookeeper-3.4.14.tar.gz
    ```
5.  部署Kafka  
    对每个服务器从官网下载压缩包：**kafka_2.11-2.3.0.tgz**
    ```
    tar -xvf kafka_2.11-2.3.0.tgz
    mv kafka_2.11-2.3.0 kafka
    cd kafka
    vim config/server.properties
    ```
    修改以下config：
    ```
    brokers.id=1 #每个server不一样，server-N该值修改为N
    zookeeper.connect=10.0.0.52:2181,10.0.0.34:2181,10.0.0.101:2181
    listeners=PLAINTEXT://10.0.0.52:9092
    delete.topic.enable=true
    log.dirs=/home/centos/kafka/kafka-logs
    ```
    ```
    cd ~
    tar -cvf kafka.tar.gz kafka
    scp kafka.tar.gz server-2:/home/centos/
    scp kafka.tar.gz server-3:/home/centos/
    ```
    分别进入另外两个服务器并解压该压缩包，修改brokers.id为相应的值，再返回server-1
    ```
    cd ~/kafka
    vim start.sh
    chmod 777 start.sh
    ```
    通过start.sh脚本来启动kafka集群
    ```
    #! /bin/bash
    echo "start server-1 kafka"
    nohup bin/kafka-server-start.sh config/server.properties > ka.log &
    echo "start local done..."
    echo ""

    echo "start slaves"
    for i in 2 3
    do
    ssh server-$i "nohup /home/centos/kafka/bin/kafka-server-start.sh /home/centos/kafka/config/server.properties > /home/centos/kafka/ka.log &"
    done
    echo "start slaves done..."

    sleep 3s
    echo "done"
    echo ""

    echo "test whether success"
    echo "server-1:"
    jps | grep Kafka
    for i in 2 3
    do
    echo "server-"$i":"
    ssh server-$i "jps | grep Kafka"
    done
    echo "test done"
    ```
    当出现以下输出时，集群启动成功
    ```
    TODO
    ```
    通过stop.sh脚本,销毁kafka集群
    ```
    #! /bin/bash
    echo "stop... beign from local kafka"
    bin/kafka-server-stop.sh
    echo "stop local done"
    echo ""

    echo "stop slaves..."
    for i in 2 3
    do
    ssh server-$i "/home/centos/kafka/bin/kafka-server-stop.sh"
    done
    echo "stop slaves done"
    echo ""

    echo "remove logs"
    rm ka.log

    for i in 2 3
    do
    ssh server-$i "rm /home/centos/kafka/ka.log"
    ssh server-$i "rm -rf /home/centos/kafka/kafka-logs"
    done
    echo "remove logs done"
    ```
6.  部署spark  
    在server-1 spark-2.4.3-bin-hadoop2.7/sbin中启动Spark的脚本

    ```
    if [ -z "${SPARK_HOME}" ]; then
    export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
    fi
    
    # Load the Spark configuration
    . "${SPARK_HOME}/sbin/spark-config.sh"
    
    # Start Master
    "${SPARK_HOME}/sbin"/start-master.sh
    
    # Start Workers
    "${SPARK_HOME}/sbin"/start-slaves.sh
    ```

    curl https://192.168.2.36:8080 结合HTML预览工具可以查看状态：

    ![Figure 2](https://raw.githubusercontent.com/zztttt/DistributedComputing/master/report/image-20190713192717887.png)



## Program design
1.  启动HttpSender
    通过window主机向server发送消息，在这里为订单
    ```
    public class HttpSenderUtil {

    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    public static  String httpSend(String url, Map<String, Object> propsMap)  throws Exception{
        HttpClient httpClient = new HttpClient();
        PostMethod postMethod = new PostMethod(url);// POST请求
        String returnString ="";
        // 参数设置
        Set<String> keySet = propsMap.keySet();
        NameValuePair[] postData = new NameValuePair[keySet.size()];
        int index = 0;
        for (String key : keySet) {
            postData[index++] = new NameValuePair(key, propsMap.get(key).toString());
        }
        postMethod.addParameters(postData);
        try {
            httpClient.executeMethod(postMethod);// 发送请求
            java.io.InputStream input = postMethod.getResponseBodyAsStream();
            returnString = convertStreamToString(input).toString();

        } catch (HttpException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            postMethod.releaseConnection();// 关闭连接
        }
        return returnString;
    }

    public static String convertStreamToString(java.io.InputStream input)
            throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input,
                "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            sb.append(line + "\n");
        }
        input.close();
        return sb.toString();
    }

    private static String url = "http://202.120.40.8:30361";

    public static void main(String[] strs){
        String filename = "C:\\Users\\loluz\\Desktop\\DS\\lab5\\src\\main\\java\\order.json";
        String contents = "";
        try {
            contents = new String(Files.readAllBytes(new File(filename).toPath()));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //Order order = new JSONObject().parseObject(contents,Order.class);
        List<Order> orderList = JSONArray.parseArray(contents.toString(),Order.class);
        for (Order od : orderList) {
            String msg = JSON.toJSONString(od);
            String param = "msg="+msg;
            String res = HttpSenderUtil.sendPost(url, param);
            System.out.println(res);
            //System.out.println(od.items);
        }
        long time=new Date().getTime();
        //String check=MD5.getMD5(time+"www.j1.com");
       /* String mobile="13053702096";
        String msg="尊敬的用户  ， 您在健一网的安全验证码为897489，健一网祝您身体健康";
        String param="t="+time+"&mobile="+mobile+"&msg="+msg;
        //String res=HttpSenderUtil.sendPost("http://localhost:8080/ec-dec/page/sms/sendSms/code",param);
        String res = HttpSenderUtil.sendPost(url, param);
        System.out.println(res);*/
    }
    /**
     * 执行get方法
     * @param url
     * @param queryString
     * @return
     */
    public static String doGet(String url, String queryString) {
        String response = null;
        HttpClient client = new HttpClient();
        HttpMethod method = new GetMethod(url);
        try {
            if (StringUtils.isNotBlank(queryString))
                method.setQueryString(URIUtil.encodeQuery(queryString));
            client.executeMethod(method);
            if (method.getStatusCode() == HttpStatus.SC_OK) {
                response = method.getResponseBodyAsString();
            }
        } catch (URIException e) {
            //logger.error("执行HTTP Get请求时，编码查询字符串“" + queryString + "”发生异常！", e); 
        } catch (IOException e) {
            //logger.error("执行HTTP Get请求" + url + "时，发生异常！", e); 
        } finally {
            method.releaseConnection();
        }
        return response;
    }
    ```

2.  启动HttpReceiver，并使用Kafka的producer将接收到的数据发给指定topic  
    ```
    public class ProducerAndRecv implements HttpHandler{
        private Properties properties;
        Producer<String, String> producer = null;

        private static final String topic = "test1";

        @Override
        public void handle(HttpExchange arg0) throws IOException
        {
            System.out.println("accept an exchange from internet.....");

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(arg0.getRequestBody()));

            String msg = bufferedReader.readLine();
            SendMsg(msg);

            String resp = "your request message i get it!";
            arg0.sendResponseHeaders(200, resp.getBytes().length);
            OutputStream out = arg0.getResponseBody();
            out.write(resp.getBytes());

            out.flush();
            arg0.close();
        }

        ProducerAndRecv(){
            Properties prop = new Properties();
            prop.put("bootstrap.servers", "10.0.0.52:9092, 10.0.0.34:9092, 10.0.0.101:9092");
            prop.put("acks", "all");
            prop.put("retries", 0);
            prop.put("batch.size", 16384);
            prop.put("linger.ms", 1);
            /*prop.put("buffer.memory", 33554432);*/
            prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            this.properties = prop;
        }

        public void SendMsg(String msg){
            //create a new producer
            producer = new KafkaProducer<String, String>(this.properties);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
            producer.send(record);

            producer.close();
        }
    }

    ```
    HttpReceiver.java:
    ```
    public class HttpReceiver
    {
        public static void main(String[] args) throws Exception
        {
            HttpServerProvider provider = HttpServerProvider.provider();
            HttpServer server = provider.createHttpServer(new InetSocketAddress(30361), 10);

            ProducerAndRecv prodRecv = new ProducerAndRecv();
            server.createContext("/", prodRecv);

            server.setExecutor(null);
            server.start();
        }
    }
    ```
3.  利用spark-Kafka的direct方式，周期性地查询Kafka，来获得每个topic+partition的最新的offset，处理数据的job启动时，就会使用Kafka的简单consumer api来获取Kafka指定offset范围的数据  
    在这里利用spark-streaming读取kafka内相应topic的消息  
    ```
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
            sparkContext.start();
            sparkContext.awaitTermination();
        }
    }
    ```
##  the problems you encountered
1.  服务器ping不同域名，但是能ping通具体IP，且在服务器之间能自由ping通  
    怀疑是DNS出现问题，但是修改了不同的DNS服务器后依然没有解决，后来发现是安全组的问题，新增了一些方法得以解决
2.  重启kafka时，logs permission denied  
    默认的kafka logs放在了需要root权限操作的地方，就是说上一次启动的集群关闭时有残留的log没有清除，在stop.sh脚本里加入对log的删除命令即可
3.  Kafka集群topic删除时只被marked as deleted but not really impact  
    在server.properties里加入新的config：
    ```
    delete.topic.enable=true
    ```
3.  spark安装运行在webUI界面不显示worker  
    在master即server-1的conf/spark-env.sh内增加了对于master的环境变量
    ```
    export SPARK_MASTER_IP=server-1
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_HOST=server-1
    ```
4.  java程序如何打包部署  
    查询资料后，得知在pom.xml里引入build配置，设定好mainClass，在项目目录下mvn assembly:assembly进行打包，在服务器上运行 **java -jar [your jar package]**
5.  kafka版本logging noMethodFound   
    就是java dependency的问题，在引入的依赖中把Scala2.12版本换成2.11版本即可
6.  Kafka的receive模式在spark2.x版本后不再进行支持  
    因为我们spark版本使用的是2.4.3，所以将消费方式改成direct就得以解决

##  the contribution of each student
| 姓名 | 学号 | 内容 |
| ------ | ------ | ------ |
| 方俊杰 | 516030910006 | cloud环境搭建，zookeeper、spark部署与使用，zookeeper分布式锁|
| 张政童 | 516030910016 | cloud环境搭建， zookeeper、Kafka部署与使用，文档编写|
| 刘泽宇 | 516030910108 | cloud环境搭建，数据库创建，order传输逻辑|
| 李翌珺 | 516030910395 | cloud环境搭建，spark部署，文档编写 |



