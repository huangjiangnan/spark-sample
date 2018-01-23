package test.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @author huangjiangnan
 *
 */
//远程连接需要实现系列化接口不然会报错
public class CountWord implements Serializable{

	private static final long serialVersionUID = -8352827020590871639L;

	public static void main(String[] args) {
		//设置spark集群上面可以连接的用户名
		System.setProperty("HADOOP_USER_NAME", "root");
		// 创建一个java版本的Spark Context并设置名称
		SparkConf conf = new SparkConf().setAppName("My App2");
		//设置主主节点及端口
		conf.setMaster("spark://192.168.7.202:7077");
		//从集群yarn中复制配置文件yarn-site.xml，在hadoop/etc/hadoop/目录下面
		conf.set("spark.yarn.dist.files", "D:\\scala-work\\spark\\src\\main\\resources\\yarn-site.xml");
		//设置本地要上传jar包，也就是本程序打的jar包，先打包后执行
		String[] jars = { "D:\\scala-work\\spark\\target\\spark-0.0.1-SNAPSHOT.jar" };
		conf.setJars(jars);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 从hadoop中hdfs读取输入数据
		JavaRDD<String> input = sc.textFile("hdfs://192.168.7.202:900/test/sql.txt");
		// 根据空格切分成单词
		JavaRDD<String> words = input.flatMap((String x) -> {
			List<String> list = Arrays.asList(x.split(" "));
			return list.iterator();
		});
		// 转换成键值对并计数
		JavaPairRDD<String, Integer> count = words.mapToPair((String x) -> {
			return new Tuple2<String, Integer>(x, 1);
		}).reduceByKey((Integer v1, Integer v2) -> {
			return v1 + v2;
		});
		// 将统计出来的单词存入一个文本文件
		count.saveAsTextFile("hdfs://192.168.7.202:900/test/sql-spark6.txt");
		sc.close();
	}

}
