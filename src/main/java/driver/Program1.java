package driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This program opens the given inputPath and performs
 * calculations on all of the files contained in the directory
 */
public class Program1 implements Serializable {

    public static final String NAME = "program1";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'-HH-mm-ss-SSSZ");

    private final JavaSparkContext _sc;
    private final SparkConf _conf;
    private final String _inputPath;
    private final String _outputPath;

    private final List<String> _results = new ArrayList<>();

    public static void main (String[] args) {
        //checkArgs (args, 1, "usage: " + NAME + "{output path}");
        Program1 program = new Program1(args[0], args[1]);
        program.process();
    }

    public Program1(String inputPath, String outputPath) {
        _inputPath = inputPath;
        _outputPath = outputPath;
        _conf = new SparkConf().setAppName(Program1.class.getSimpleName());
        _sc = new JavaSparkContext(_conf);
    }

    public void process() {
        JavaRDD<String> lines = _sc.textFile(_inputPath).cache();

        _conf.log().info("START");
        _results.add("processing input file : " + _inputPath);
        _results.add("total data set line recordCount : " + computeLineCount(lines));
        _results.add("total data set byte size : " + computeTotalSize(lines));

        saveResults();
    }

    public void saveResults() {
        // making into an RDD in order to save it
        JavaRDD<String> distResults = _sc.parallelize(_results);
        String fileName = NAME + "-" + DATE_FORMAT.format(new Date());
        distResults.saveAsTextFile(_outputPath + "/" + fileName);
    }

    public long computeLineCount(JavaRDD<String> lines) {
        // Calculate number of records in the data set
        return lines.count();
    }

    public long computeTotalSize(JavaRDD<String> lines) {
        // Calculate size of the data set
        // map each line of the file to its length
        JavaRDD<Integer> lineLengths = lines.map(new FcnStringToLength());

        // reduce the set of line sizes to a total length of the entire file
        return lineLengths.reduce(new FcnNumbersToSum());
    }

    public static class FcnStringToLength implements Function<String, Integer> {
        public Integer call(String s) {
            return s.length();
        }
    }

    public static class FcnNumbersToSum implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) {
            return a + b;
        }
    }

}
