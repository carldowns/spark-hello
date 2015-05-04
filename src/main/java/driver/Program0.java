package driver;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class Program0 implements Serializable {

    public static final String NAME = "program9";
    private final JavaSparkContext _sc;
    private final SparkConf _conf;
    private final String _inputPath;
    private final String _outputPath;

    private static Logger _log = Logger.getLogger(Program0.class);
    private final StringBuffer _results = new StringBuffer();


    public Program0(String inputPath, String outputPath) {
        _conf = new SparkConf().setAppName(Program0.class.getSimpleName());
        _sc = new JavaSparkContext(_conf);
        _inputPath = inputPath;
        _outputPath = outputPath;
    }

    public void process() {
        _log.info("not implemented");
        _results.append("processing input directory : ").append(_inputPath);
        throw new NotImplementedException("");

        // at inputPath:
        // build a List<StashDirectory>
        // Bill suggests this will require an S3 API or other approach
        // probably right.

        // we can test everything else by just passing in a list of buckets.

        // Spark-func :: build a Lines RDD for each directory
        // store to Map<StashDirectory,Lines>

        // Spark-func :: map each Line to a size
        // store to Map<StashDirectory,Size>

        // Spark-func :: map each Line to a recordCount
        // store to Map<StashDirectory,Count>

        // collect each of the RDDs.
        // iterate, write data to a combined EmoTableStat
        // store in Map<StashDirectory,EmoTableStat>

        // Spark-func :: map each EmoTableStat to Json
        // store in Map<StashDirectory,Json>

        // collect, persist

    }

}
