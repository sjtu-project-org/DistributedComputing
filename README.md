# Lab5 

## System environment
1. Host: 4 virtual servers(centos7) with 4 VCPUs and 8GB RAM is set up
   ![Figure 1](https://raw.githubusercontent.com/zztttt/DistributedComputing/master/report/image-20190713144347113.png)
2. Http request sender: （Windows 的系统配置数据 #TODO ）

## Install and configuration process
1. openstack instance配置与SSH免密登陆：  
    在/.ssh/目录下生成config文件：
    ``` bash
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
    修改zookeeper.properties
    ``` bash
    dataDir=/home/centos/kafka/zookeeper
    tickTime=2000
    initLimit=5
    syncLimit=2
    server.1=10.0.0.52:2888:3888
    server.2=10.0.0.34:2888:3888
    server.3=10.0.0.101:2888:3888
    ```
    通过startZKCluster.sh启动zookeeper
    ``` bash
    #! /bin/sh
    echo "start zookeeper ....."
    /home/centos/zookeeper-3.4.14/bin/zkServer.sh start
    for i in 2 3
    do
    ssh server-$i "/home/centos/zookeeper-3.4.14/bin/zkServer.sh start"
    done

    sleep 3s

    echo server-1
    /home/centos/zookeeper-3.4.14/bin/zkServer.sh status
    for i in 2 3
    do
    echo server-$i
    ssh server-$i "/home/centos/zookeeper-3.4.14/bin/zkServer.sh status"
    done
    echo "zookeeper start ok"
    ```
    通过stopZKCluster.sh关闭zookeeper
    ``` bash
    #! /bin/sh
    echo "stop zookeeper ..."
    /home/centos/zookeeper-3.4.14/bin/zkServer.sh stop
    for i in 2 3
    do
    ssh server-$i "/home/centos/zookeeper-3.4.14/bin/zkServer.sh stop"
    done
    echo "zookeeper stoped"
    ```
5.  部署Kafka  
    对每个服务器从官网下载压缩包：**kafka_2.11-2.3.0.tgz**
    ``` bash
    tar -xvf kafka_2.11-2.3.0.tgz
    mv kafka_2.11-2.3.0 kafka
    cd kafka
    vim config/server.properties
    ```
    修改以下config：
    ``` bash
    brokers.id=1 #每个server不一样，server-N该值修改为N
    zookeeper.connect=10.0.0.52:2181,10.0.0.34:2181,10.0.0.101:2181
    listeners=PLAINTEXT://10.0.0.52:9092
    delete.topic.enable=true
    log.dirs=/home/centos/kafka/kafka-logs
    ```
    ``` bash
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
    ``` bash
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
    ``` bash
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
6.  部署 Spark
    从官网下载 Spark 压缩包到服务器并解压
    配置 server-1 spark-2.4.3-bin-hadoop2.7/conf 目录下的 spark-env.sh
    ``` bash
    SPARK_LOCAL_IP=server-1
    SPARK_LOCAL_DIRS=/home/centos/spark-2.4.3-bin-hadoop2.7/tmp
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.212.b04-0.el7_6.x86_64

    export SPARK_MASTER_IP=server-1
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_HOST=server-1
    ```
    然后使用 scp 命令传输整个 spark 目录到 server-2 和 server-3，注意修改其中 **SPARK_LOCAL_IP** 的值对应服务器IP

    在server-1 spark-2.4.3-bin-hadoop2.7/sbin中执行启动Spark的脚本
    ```
    sh sbin/start-all.sh
    ```
    即可启动 三个 server（1-3） 的 master-salves spark 集群
    执行
    ```
    curl https://192.168.2.36:8080 
    ```
    结合HTML预览工具可以查看状态：

    ![Figure 2](https://raw.githubusercontent.com/zztttt/DistributedComputing/master/report/image-20190713192717887.png)


## Program design
0.  系统运行流程为：从 Windows 主机使用 HttpSender 向 Cloud Server 映射到外网的端口 30361 发送订单信息。在 server 中存在一个 Http Recevier 监听该端口，获取到 Order 信息并使用 Kafka Producer 向 Kafka 相应 topic 生产消息。Spark 中运行的 Spark Streaming 任务使用 Kafka API 使用 Direct 方式消费消息，完成订单的处理逻辑并持久化到数据库，同时处理 Zookeeper 管理的总交易额数据。

    另外，在 Recevier 接受到消息时利用 Zookeeper 生成一个唯一的 orderID 作为返回值响应请求，用户后续可以使用该 orderID 查询订单完成情况。同时还有四个线程每隔一定时间修改 Zookeeper 管理的汇率数据。

1.  启动HttpSender
    通过 window 主机向 server 发送订单消息
    ``` Java
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

    private static String url = "http://202.120.40.8:30361";

    public static void main(String[] strs){
        String filename = "Test File Path";
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
            String param = msg;
            String res = HttpSenderUtil.sendPost(url, param);
            System.out.println(res);
            //System.out.println(od.items);
        }
        long time=new Date().getTime();
    }
    ```

2.  启动HttpReceiver，并使用 Kafka 的 producer 将接收到的数据发给指定 topic，注意其中向 Zookeeper 请求了一个全局唯一的 OrderID
    HttpReceiver.java:
    ``` Java
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
    ProducerAndRecv.java
    ``` Java
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

            /* TODO: add a order id generator */
            String orderPath = null;
            try {
                orderPath = zkClient.create(ORDER_PATH,
                        Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println(orderPath);

            } catch (KeeperException | InterruptedException e) {
                System.out.println("HTTP handler orderID wrong");
            }

            String orderId = orderPath.substring(ORDER_PATH.length());

            String Msg2Kafka = String.format("{order_id:%s,order:%s}", orderId, msg);
            SendMsg2Kafka(Msg2Kafka);

            String resp = "your request message already received," + "orderID : "+ orderId;
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
   
3.  利用spark-Kafka的direct方式，周期性地查询Kafka，来获得每个topic+partition的最新的offset，处理数据的job启动时，就会使用Kafka的简单consumer api来获取Kafka指定offset范围的数据  
    在这里利用spark-streaming读取kafka内相应topic的消息  
    ``` Java
    public class JavaDirectDemo {

        private static final Pattern SPACE = Pattern.compile(" ");

        private static final String topic = "test1";

        ... 
        // Spark Zookeepr Mysql lockServer 的连接器

        public static void main(String[] args) throws Exception{
            // 初始化 Spark Zookeepr Mysql 等连接
            ...


            Collection<String> topics = Arrays.asList(topic);

            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            sparkContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );

            // Order 处理逻辑
            JavaDStream<OrderWithID> orderDStream = stream.map(
                new Function<ConsumerRecord<String, String>, OrderWithID>() {
                    @Override
                    public OrderWithID call(ConsumerRecord<String, String> cr) throws Exception {
                        System.out.println(cr.key());
                        System.out.println(cr.value());
                        return JSONObject.parseObject(cr.value(), OrderWithID.class);
                    }
                }).cache();
            // 处理每个 RDD 中的 order
            orderDStream.foreachRDD(
                new VoidFunction<JavaRDD<OrderWithID>> () {
                    @Override
                    public void call(JavaRDD<OrderWithID> rdd) throws Exception {
                        List<OrderWithID> orders = rdd.collect();
                        for (OrderWithID o : orders) {
                            System.out.println(o.getOrder_id());
                            handleOrder(o);
                        }
                    }
                }
            );

            sparkContext.start();
            sparkContext.awaitTermination();
            }
        }

        private static void handleOrder(OrderWithID o) throws Exception {
            // 获取锁

            // 从Mysql读取数据

            // 按订单处理逻辑处理

            // 持久化结果表，更新商品库存到 Mysql

            // 释放锁
        }
    }
    ```
4. 汇率每隔一段时间随机变化
``` JAVA
public class mySimple {

    private static ZooKeeper authZK = null;
    private static final String intraHost = "10.0.0.52:2181";

    private static Double d = 2.0;
    private static final Double[] currencyBase = new Double[]{2.0, 12.0, 0.15, 9.0};
    private static String[] currencyType = new String[]{"RMB", "USD", "JPY", "EUR"};
    private static final int[] currencyLow = new int[]{140, 840, 10, 630};
    private static final int[] currencyHigh = new int[]{260, 1560, 20, 1170};

    public static void main(String[] args) throws KeeperException, InterruptedException,Exception {
        System.out.println("in main");

        final CountDownLatch latch = new CountDownLatch(1);
        authZK = new ZooKeeper(intraHost, 50000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("ZK connected");
                try {
                    for (int i=0; i<4; i++) {
                        //authZK.create("/Currency/"+currencyType[i], double2Bytes(d), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        //如果根节点不存在，则创建该货币节点
                        Stat stat = authZK.exists("/Currency/"+currencyType[i], false);
                        if (stat == null) {
                            authZK.create("/Currency/"+currencyType[i], double2Bytes(currencyBase[i]), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                        final int index = i;
                        final long interval = 15000;
                        new Thread(() -> {
                            try {
                                while (true) {
                                    int low = currencyLow[index];
                                    int high = currencyHigh[index];
                                    Random rand = new Random();
                                    double newValue = (rand.nextInt(high-low+1)+low) / 100.0;
                                    authZK.setData("/Currency/"+currencyType[index], double2Bytes(newValue), -1);
                                    byte[] data = authZK.getData("/Currency/"+currencyType[index], false, null);
                                    System.out.println("Currency type: "+ currencyType[index]+" currency value: "+bytes2Double(data));
                                    Thread.sleep(interval);
                                }

                            } catch (Exception e) {

                            }
                        }).start();
                    }
                }
            }
        });
        latch.await();

    }
```
5. 分布式锁实现
``` JAVA
public class FairLock {

    //ZooKeeper配置信息
    private ZooKeeper zkClient;
    private static final String interHost = "10.0.0.52:2181";

    // all lock is child of /Locks;
    private String LOCK_ROOT_PATH = "/FairLocks";

    private static final String LOCK_NODE_NAME = "Lock_";
    private String lockPath;

    // 监控lockPath的前一个节点的watcher
    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.out.println(event.getPath() + " 前锁释放");
            synchronized (this) {
                notifyAll();
            }

        }
    };

    public FairLock(String LockWhat) throws IOException, InterruptedException, KeeperException{
        this.LOCK_ROOT_PATH = this.LOCK_ROOT_PATH + "/" + LockWhat;

        // ZKClient 连接初始化
    }

    //获取锁的原语实现.
    public  void acquireLock() throws InterruptedException, KeeperException {
        //创建锁节点
        createLock();
        //尝试获取锁
        attemptLock();
    }

    private void createLock() throws KeeperException, InterruptedException {
        // 创建EPHEMERAL_SEQUENTIAL类型节点
        String lockPath = zkClient.create(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME,
                Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(Thread.currentThread().getName() + " 锁创建: " + lockPath);
        this.lockPath=lockPath;
    }

    private void attemptLock() throws KeeperException, InterruptedException {
        // 获取Lock所有子节点，按照节点序号排序
        List<String> lockPaths = null;
        lockPaths = zkClient.getChildren(LOCK_ROOT_PATH, false);
        Collections.sort(lockPaths);
        int index = lockPaths.indexOf(lockPath.substring(LOCK_ROOT_PATH.length() + 1)); //TODO: need this.?

        // 如果lockPath是序号最小的节点，则获取锁
        if (index == 0) {
            System.out.println(Thread.currentThread().getName() + " 锁获得, lockPath: " + lockPath);
            return ;
        } else {
            // lockPath不是序号最小的节点，监控前一个节点
            String preLockPath = lockPaths.get(index - 1);
            Stat stat = zkClient.exists(LOCK_ROOT_PATH + "/" + preLockPath, watcher);
            // 假如前一个节点不存在了，比如说执行完毕，或者执行节点掉线，重新获取锁
            if (stat == null) {
                attemptLock();
            } else { // 阻塞当前进程，直到preLockPath释放锁，被watcher观察到，notifyAll后，重新acquireLock
                System.out.println(" 等待前锁释放，prelocakPath："+preLockPath);
                synchronized (watcher) {
                    watcher.wait();
                }
                attemptLock();
            }
        }
    }

    //释放锁的原语实现
    public void releaseLock() 
}
```
6. 发送获取订单完成情况与获取某货币总交易额的请求
``` JAVA

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
4.  spark安装运行在webUI界面不显示worker  
    在master即server-1的conf/spark-env.sh内增加了对于master的环境变量
    ```
    export SPARK_MASTER_IP=server-1
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_HOST=server-1
    ```
5.  java程序如何打包部署  
    查询资料后，得知在pom.xml里引入build配置，设定好mainClass，在项目目录下mvn assembly:assembly进行打包，在服务器上运行 **java -jar [your jar package]**
6.  kafka： logging noMethodFound   
    就是java dependency版本的问题，在引入的依赖中把Scala2.12版本换成2.11版本即可
7.  Kafka的receive模式在spark2.x版本后不再进行支持  
    因为我们spark版本使用的是2.4.3，所以将消费方式改成direct就得以解决
8.  写java类的时候，把一些成员变量误设为static类型，导致成员变量管理混乱  
    去掉static即可

##  the contribution of each student
| 姓名 | 学号 | 内容 |
| ------ | ------ | ------ |
| 方俊杰 | 516030910006 | HTTP服务搭建，zookeeper部署、spark部署与使用，zookeeper分布式锁, Spark SQL 持久化|
| 张政童 | 516030910016 | HTTP服务搭建， zookeeper部署、Kafka部署与使用，文档编写|
| 刘泽宇 | 516030910108 | HTTP传输order，数据库创建，JSON模块实现，Zookeeper汇率变化实现|
| 李翌珺 | 516030910395 | cloud环境搭建，spark部署，文档编写 |



