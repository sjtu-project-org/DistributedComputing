import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SparkMysql {
    public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkMysql.class);

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
                .setAppName("SparkMysql")
                .setMaster("spark://server-1:7077"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        //读取mysql数据
        readMySQL(sqlContext);

        // 写入 mysql 数据
        writeMySQL(sqlContext, sparkContext);

        //停止SparkContext
        sparkContext.stop();
    }
    private static void readMySQL(SQLContext sqlContext){
        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://10.0.0.37:3306/dslab";
        //查找的表名
        String table = "commodity";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","123");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取dslab数据库中的commodity表内容");
        // 读取表中所有数据
        Dataset<Row> jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
        //显示数据
        jdbcDF.show();
    }

    private static void writeMySQL(SQLContext sqlContext, JavaSparkContext sparkContext) {
        String url = "jdbc:mysql://10.0.0.37:3306/dslab";
        //查找的表名
        String table = "result";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","123");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //写入的数据内容
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("1 12345 USD 1 792.15"));
        /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         */
        //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                //return RowFactory.create(Integer.valueOf(splited[0]),splited[1],Integer.valueOf(splited[2]));
                return RowFactory.create(
                        Integer.valueOf(splited[0]),
                        splited[1],
                        splited[2],
                        Integer.valueOf(splited[3]),
                        Double.valueOf(splited[4])
                );
            }
        });

        /**
         * 第二步：动态构造DataFrame的元数据。
         */
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("user_id",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("initiator",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("success",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("paid",DataTypes.DoubleType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         */
        Dataset<Row> personsDF = sqlContext.createDataFrame(personsRDD,structType);

        /**
         * 第四步：将数据写入到person表中
         */
        personsDF.write().mode("append").jdbc(url,"result",connectionProperties);


    }
}