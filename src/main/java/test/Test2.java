package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

public class Test2 implements Serializable {

    public static void main(String[] args) {

        // This works for a specific file:
        new Test2().processS3File("s3n://emodb-us-east-1/stash/ci/2015-04-10-00-00-00/answer-adobe");

        // Does not work with directories as in this case.
        // new S3SparkApp().process("s3n://emodb-us-east-1/stash/ci/2015-04-10-00-00-00");
        // Exception in thread "main" java.io.IOException: Not a file: s3n://emodb-us-east-1/stash/ci/2015-04-10-00-00-00/agrippa-displaycodeoverride
        // that is telling us that as coded, it expects the input to a directory that contains files.
        // we we have in the below is a directory of subdirectories and this barfs the first directory it hits
        // namely agrippa-displaycodeoverride
    }

    private SparkConf _conf;
    private JavaSparkContext _sc;

    public Test2() {
        _conf = new SparkConf().setAppName("Spark Stash Application");
        _sc = new JavaSparkContext(_conf);
    }

    public void processS3File(String s3FilePath) {
        System.out.println("processing specific S3 file : " + s3FilePath);
        JavaRDD<String> lines = _sc.textFile(s3FilePath).cache();
        System.out.println("total count = " + getLineCount(lines));
        System.out.println("total size = " + outputTotalSize(lines));
    }

    public long getLineCount (JavaRDD<String> lines) {
        // Calculate number of records in the data set
        return lines.count();
    }

    public long outputTotalSize (JavaRDD<String> lines) {
        // Calculate size of the data set

        // map each line of the file to its length
        JavaRDD<Integer> lineLengths = lines.map(new StringToLengthFcn());

        // reduce the set of line sizes to a total length of the entire file
        return lineLengths.reduce(new NumbersToSumFcn());
    }

    public static class StringToLengthFcn implements Function<String, Integer> {
        public Integer call(String s) {
            return s.length();
        }
    }

    public static class NumbersToSumFcn implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) {
            return a + b;
        }
    }

}
