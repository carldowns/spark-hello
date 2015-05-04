package test;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class Test1 {
    public static void main(String[] args) {

        //String logFile = "file:///Users/carl.downs/work/dev/bv/stash-hello/data/README.md"; // Should be some file on your system
        String inputFile = "file:///Users/carl.downs/work/dev/bv/stash-hello/data/input1.txt"; // Should be some file on your system

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(inputFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
