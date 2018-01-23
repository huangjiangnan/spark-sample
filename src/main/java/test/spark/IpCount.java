package test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @author huangjiangnan 单词统计，需要打jar包到集群，然后shell提交任务到spark
 */

public class IpCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("spark://192.168.7.202:7077").setAppName(IpCount.class.getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputRDD = sc.textFile("hdfs://192.168.7.202:900/test/nohup*").repartition(2);
		// java lambda表达式 jdk8以上，省很多代码
		// 转化RDD，过滤，只需要想要的行
		JavaRDD<String> reqRDD = inputRDD.filter((String x) -> {
			if (x.contains("请求head")) {
				return true;
			}
			return false;
		});

		// JavaPairRDD 建值对
		JavaPairRDD<String, Integer> pairRDD = reqRDD.mapToPair((String x) -> {
			String[] ss = x.split(",");
			String ip = "未知ip";
			for (String st : ss) {
				if (st.contains("x-forwarded-for")) {
					String[] ipStr = st.split("=");
					if (ipStr.length > 1) {
						ip = ipStr[1];
						break;
					}
				}
			}
			return new Tuple2<String, Integer>(ip, 1);
		}).reduceByKey((Integer num1, Integer num2) -> {
			return num1 + num2;
		});
		pairRDD.saveAsTextFile("hdfs://192.168.7.202:900/test/FilterLine-spark");
		sc.close();
	}
}
