package driver;

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
 * simply pulls in the list of records associated with the leaf files available.
 * Use can use a * wildcard at the higher level to group all records together
 * for example the top entry will include all records from all files contained in the sub-directories
 *
 * spark-submit --class "driver.Program6" --master local[4] target/spark-hello-1.0-SNAPSHOT.jar
 * file:///Users/carl.downs/work/dev/bv/spark-hello/log
 * file:///Users/carl.downs/work/dev/bv/stash-data/*
 *
 * spark-submit --class "driver.Program6" --master local[4] target/spark-hello-1.0-SNAPSHOT.jar
 * file:///Users/carl.downs/work/dev/bv/spark-hello/log
 * file:///Users/carl.downs/work/dev/bv/stash-data/answer-1800petmeds
 *
 * nice but does not help us REMEMBER the list of directories that went into the resulting RDD
 *
 */
public class Program6 extends AbstractProgram implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    public static final String NAME = Program6.class.getSimpleName();

    private final JavaSparkContext _sc;

    private final String _outputPath;
    private final String _targetPath;

    private static final Logger _log = Logger.getLogger(Program6.class);
    private final List<String> _results = new ArrayList<>();

    public static void main (String[] args) {
        checkArgs (args, 2, "usage: " + NAME + "{output path} {input path}");
        Program6 program = new Program6(args);
        program.process();
    }

    public Program6(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Program6.class.getSimpleName());
        _sc = new JavaSparkContext(conf);

        _outputPath = args[0];
        _targetPath = args[1];

        _log.info("output path: " + _outputPath);
        _log.info("target path: " + _targetPath);
    }

    public void process() {

        try {
            JavaRDD<String> files =  _sc.textFile(_targetPath);
            Iterator<String> i = files.toLocalIterator();
            _log.info (" recordCount: " + files.count());
//            while (i.hasNext()) {
//                _log.info (" value: " + i.next());
//            }
        }
        catch (Exception e) {
            _log.error("problem processing ", e);
        }
    }

}
