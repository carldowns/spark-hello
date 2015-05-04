package driver;

import amazon.S3NativeAccessor;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This program lists S3 sub-directories relative to target path
 * example: s3n://emodb-us-east-1/stash/ci/2015-04-16-00-00-00/
 *
 * This is working locally but fails in the EMR cluster due to AWS key/secret failures for s3n
 */
public class Program4 implements Serializable {

//    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
//    public static final String NAME = "program4";
//
//    private final JavaSparkContext _sc;
//
//    private final String _outputPath;
//    private final String _targetPath;
//
//    private static Logger _log = Logger.getLogger(Program4.class);
//    private final List<String> _results = new ArrayList<>();
//
//    public Program4(String[] args) {
//
//        SparkConf conf = new SparkConf().setAppName(Program4.class.getSimpleName());
//        _sc = new JavaSparkContext(conf);
//
//        _outputPath = args[1];
//        _targetPath = args[2];
//
//        _sc.getConf().log().info("output path {}", _outputPath);
//        _sc.getConf().log().info("target path {}", _targetPath);
//    }
//
//    public void process() {
//
//        try {
//            _sc.getConf().log().info("attempting to list directories for {}", _targetPath);
//
//            // list the directories of the target path
//            S3NativeAccessor s3 = new S3NativeAccessor(_sc, _targetPath);
//            for (Path path : s3.getDirectories(s3.getWorkingDirectory())) {
//                //_sc.getConf().log().info(path.getName());
//                _results.add(path.getName());
//            }
//
//            _sc.getConf().log().info("results size: {}", _results.size());
//            saveResults();
//        }
//        catch (Exception e) {
//            _log.error("problem processing ", e);
//        }
//    }
//
//    public void saveResults() {
//
//        // making into an RDD in order to save it
//        JavaRDD<String> distResults = _sc.parallelize(_results);
//        String fileName = NAME + "-" + DATE_FORMAT.format(new Date());
//        distResults.saveAsTextFile(_outputPath + "/" + fileName);
//    }
}
