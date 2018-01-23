package test.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CountWord implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8352827020590871639L;

	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "root");
		// 创建一个java版本的Spark Context
		SparkConf conf = new SparkConf().setAppName("My App2");
		conf.setMaster("spark://192.168.7.202:7077");
		conf.set("spark.yarn.dist.files", "D:\\scala-work\\spark\\src\\main\\resources\\yarn-site.xml");
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
