package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 */
public class WordCountTest implements Serializable {


    public static void main(String[] args) {
        new WordCountTest().process();
    }

    public WordCountTest() {
    }

    private void process () {

        List<String> list = Arrays.asList(
                "this is a test",
                "this is the test",
                "this is another test",
                "this is about the test",
                "this is not a test");

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // convert list to RDD
        JavaRDD<String> stringsRDD = sparkContext.parallelize(list);

        // flat map converting strings to words
        JavaRDD<String> wordsRDD = stringsRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // convert words to pair of {word, 1}
        JavaPairRDD<String,Integer> onesRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // reduce ones count by reducing {word,1} to {word,count} by key.
        JavaPairRDD<String,Integer> countsRDD = onesRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        List<Tuple2<String,Integer>> results = countsRDD.collect();
        for (Tuple2<?,?> pair : results ) {
            System.out.println(pair._1 + " : " + pair._2);
        }
    }

}
