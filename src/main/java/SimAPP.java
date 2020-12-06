/**
 * Created by 金伟华 on 2017/12/24.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;

public class SimAPP {
    public static void main(String[] args){
        String path = "E:\\备份software\\spark-2.0.1-bin-hadoop2.7\\README.md";
        //SparkSession spark = SparkSession.builder().appName("simApp").master("local[2]").getOrCreate();
        //Dataset<String> ds = spark.read().textFile(path).cache();
        SparkConf conf = new SparkConf().setAppName("simApp").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile(path).cache();
        //直接计数，计算行中含字母a和b的数量
        //long numA = ds.filter((FilterFunction<String>) {line -> conf.contains()}).count(); //不可行，filter的二义性
        //单词切分
        /*
        JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) throws Exception {
                Iterable iter = Arrays.asList(s.split(" "));
                return iter;   //required Iterator
            }
        });*/

        long numA = rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("a");
            }
        }).count();
        long numB = rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("b");
            }
        }).count();

        System.out.println("含有字母A的行数：" + numA + "\n含有字母B的行数：" + numB);

        System.out.println("Hello Spark");
    }
}
