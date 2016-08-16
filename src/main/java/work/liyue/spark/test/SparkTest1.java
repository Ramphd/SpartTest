package work.liyue.spark.test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

/**
 * Created by hzliyue1 on 2016/8/16,9:42.
 */
public class SparkTest1 {
    /**
     * The first thing a Spark program must do is to create a JavaSparkContext object,
     * which tells Spark how to access a cluster.
     * To create a SparkContext you first need to build a SparkConf object
     * that contains information about your application.
     * <p>
     * The appName parameter is a name for your application to show on the cluster UI. master is a Spark,
     * Mesos or YARN cluster URL, or a special “local” string to run in local mode.
     * <p>
     * spark://HOST:PORT	Connect to the given Spark standalone cluster master.
     * The port must be whichever one your master is configured to use, which is 7077 by default.
     */
    SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("211.87.227.209:7077");
    JavaSparkContext sc = new JavaSparkContext(conf);
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> distData = sc.parallelize(data);

}
