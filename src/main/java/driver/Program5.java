package driver;

import amazon.S3NativeAccessor;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * uses wholeTextFiles to get a tuple of files in the given directory.
 */
public class Program5 extends AbstractProgram implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    public static final String NAME = Program5.class.getSimpleName();

    private final JavaSparkContext _sc;

    private final String _outputPath;
    private final String _targetPath;

    private static final Logger _log = Logger.getLogger(Program5.class);
    private final List<String> _results = new ArrayList<>();

    public static void main (String[] args) {
        checkArgs (args, 2, "usage: " + NAME + "{output path} {input path}");
        Program5 program = new Program5(args);
        program.process();
    }

    public Program5(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Program5.class.getSimpleName());
        _sc = new JavaSparkContext(conf);

        _outputPath = args[0];
        _targetPath = args[1];

        _log.info("output path: " + _outputPath);
        _log.info("target path: " + _targetPath);
    }

    public void process() {

        try {
            // JavaSparkContext.wholeTextFiles lets you read a directory containing multiple small
            // text files, and returns each of them as (filename, content) pairs.
            // This is in contrast with textFile, which would return one record per line in each file.

            JavaPairRDD<String, String> files =  _sc.wholeTextFiles(_targetPath);
            Iterator<Tuple2<String, String>> tuple2Iterator = files.toLocalIterator();
            while (tuple2Iterator.hasNext()) {
                Tuple2<String, String> tuple = tuple2Iterator.next();
                _log.info("tuple key: " + tuple._1);
                _results.add(tuple._1);
            }
            saveResults();
        }
        catch (Exception e) {
            _log.error("problem processing ", e);
        }
    }

    public void saveResults() {

        // making into an RDD in order to save it
        JavaRDD<String> distResults = _sc.parallelize(_results);
        String fileName = NAME + "-" + DATE_FORMAT.format(new Date());
        distResults.saveAsObjectFile(_outputPath + "/" + fileName);
    }
}
