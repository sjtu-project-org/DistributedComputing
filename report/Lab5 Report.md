## Lab5 Report

Group 1 

|      |              |
| :--: | :----------: |
|      |              |
|      |              |
|      | 516030910395 |



####System environment

1. Host: 4 virtual servers(centos7) with 4 VCPUs and 8GB RAM is set up

   ![Figure 1](https://raw.githubusercontent.com/zztttt/DistributedComputing/master/report/image-20190713144347113.png)

2. Http request sender: （Windows 的系统配置数据 #TODO ）

####Install and configuration process

1. openstack instance配置与SSH免密登陆：按照要求文档逐步操作

1. ```bash
   Host centos-ext
   HostName 202.120.40.8
   User centos
   Port 30360
   IdentityFile /Users/darlenelee/.ssh/test3.pem
   
   Host centos-in2
   HostName 10.0.0.34
   User centos
   Port 22
   ProxyCommand ssh centos-ext -W %h:%p
   IdentityFile /Users/darlenelee/.ssh/test3.pe
   
   Host centos-in3
   HostName 10.0.0.101
   User centos
   Port 22
   ProxyCommand ssh centos-ext -W %h:%p
   IdentityFile /Users/darlenelee/.ssh/test3.pem
   
   Host centos-in4
   HostName 10.0.0.37
   User centos
   Port 22
   ProxyCommand ssh centos-ext -W %h:%p
   IdentityFile /Users/darlenelee/.ssh/test3.pem
   ```

2. 安装JDK和MySQL：直接通过包管理工具yum在centos上安装即可

3. 部署Zookeeper Kafka和Spark

   1. 分别从官网下载

   2. 在server-1，kafka/config/server.properties中，修改：

      ```
      brokers.id=1 #每个server不一样，server-N该值修改为N
      zookeeper.connect=10.0.0.52:2181,10.0.0.34:2181,10.0.0.101:2181
      listeners=PLAINTEXT://10.0.0.52:9092
      delete.topic.enable=true
      log.dirs=/home/centos/kafka/kafka-logs
      ```

   3. 在server-2 server-3 也做相似的修改

   4. 启动kafka集群的start.sh脚本

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

   5. 停止kafka集群的stop.sh脚本

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

   6. 在server-1 spark-2.4.3-bin-hadoop2.7/sbin中启动Spark的脚本

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

4. 

####Program design

httpSender/src/main/java/HttpSenderUtil.java：以JSON格式发送HTTP POST请求，其中要发送的.json存储在order.json中，通过转换为元素为Order类的List逐个发送文件内的订单

kafkaAPI/src/main/java/ProducerAndRecv.java：配置了kafka参数并初始化新建生产者，接受网络请求，作为生产者给kafka发消息，并返回200

kafkaAPI/src/main/java/Consumer.java：配置了kafka参数并初始化消费者，接受给定topic的消息队列并逐个处理

sparkDemo/src/main/java/KafkaReceiverWordCount.java：通过Spark的kafka api，创建针对kafka的输入数据流，放入指定topic

sparkConnectMysql/src/main/java/SparkMysql.java：使用了spark 的SQLContext api，将spark连接到MySQL数据库进行可持久化操作

####Problems encountered

1. 起初阶段，使用本地的private key一直不能成功通过SSH连接到实例，可能是DNS问题或者是openstack平台上的不稳定

2. 没有服务器集群上开发经验，流程上遭遇许多迷惑，比如如何将java程序打包部署、kafka和spark streaming之间如何收发消息等

3.  #### todo

4. 互联网上相关资源以Scala作开发工具居多，而Java稍少，对阅读和调试过程不太方便

####Contributions

1. 
2.  
3.  
4. 

####References (just list a few...)

<https://www.oschina.net/question/2609510_2171867>

<https://blog.csdn.net/u012689336/article/details/53261721>

<https://spark.apache.org/docs/1.4.0/streaming-kafka-integration.html>

<https://www.cnblogs.com/xlturing/p/6246538.html#spark-streaming%E6%8E%A5%E6%94%B6kafka%E6%95%B0%E6%8D%AE>

<https://blog.csdn.net/lyhkmm/article/details/88398102>

[https://zhuanlan.zhihu.com/p/65223757?utm_source=qq&utm_medium=social&utm_oi=37779799015424](USER_CANCEL)

<https://github.com/code4wt/distributed_lock>

<https://stackoverflow.com/questions/7181534/http-post-using-json-in-java>