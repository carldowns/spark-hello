package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by carl.downs on 4/24/15.
 */
public class WholeTextFileTest {

    String targetDirectory;

    public static void main(String[] args) {
        new WholeTextFileTest(args[0]).process();
    }

    public WholeTextFileTest(String targetDirectory) {
        this.targetDirectory = targetDirectory;
    }

    private void process() {
        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String,String> files = sparkContext.wholeTextFiles(targetDirectory);
        java.util.List<Tuple2<String,String>> local = files.collect();

        System.out.println("list size : " + local.size());
        for (Tuple2<?,?> tuple : local) {
            // same thing
            //System.out.println("filename : " + tuple._1 + " -- record : " + tuple._2);
            System.out.println("filename : " + tuple._1() + " -- record : " + tuple._2());
        }
    }
}
