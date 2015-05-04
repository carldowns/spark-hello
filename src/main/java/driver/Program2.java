package driver;

import org.apache.log4j.Logger;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This program accepts a list of emo stash directories, outputs files containing per table stats.
 */
public class Program2 implements Serializable {

    public static final String NAME = "program2";
    private final JavaSparkContext _sc;
    private final SparkConf _conf;
    private final String _outputPath;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");

    private static Logger _log = Logger.getLogger(Program2.class);
    private final List<String> buckets = new ArrayList<>();
    private final List<String> _results = new ArrayList<>();

    public Program2(String[] args) {
        _conf = new SparkConf().setAppName(Program2.class.getSimpleName());
        _sc = new JavaSparkContext(_conf);

        _outputPath = args[1];

        for (int x = 2; x < args.length; x++) {
            buckets.add(args[x]);
        }
    }

    public void process() {
        _log.info(NAME + " running");


        // Spark-func :: build a tableRecords RDD for each directory

        Map<String, EmoStatistic> statMap = new ConcurrentHashMap<>();
        Map<String, JavaRDD<String>> bucketMap = new ConcurrentHashMap<>();
        for (String bucket : buckets) {
            _log.info(" ++++++++++ processing bucket " + bucket);
            JavaRDD<String> tableRecords = _sc.textFile(bucket);
            bucketMap.put(bucket, tableRecords);

            // think this will work but it is not leveraging all of
            // the parallel processing capabilities of Spark I think

            EmoStatistic stat = new EmoStatistic ();
            statMap.put(bucket, stat);
            stat.setEmoTable(bucket);
            stat.setRecordCount(computeRecordCount(tableRecords));
            stat.setTableSize(computeTotalSize(tableRecords));
        }


        // rudimentary output
        for (EmoStatistic stat : statMap.values()) {
            _results.add(stat.toString());
        }

        saveResults();
    }

    public void saveResults() {
        // making into an RDD in order to save it
        JavaRDD<String> distResults = _sc.parallelize(_results);
        String fileName = NAME + "-" + DATE_FORMAT.format(new Date());
        distResults.saveAsTextFile(_outputPath + "/" + fileName);
    }

    public long computeRecordCount(JavaRDD<String> lines) {
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

    public static class EmoStatistic implements Serializable {
        private String emoTable;
        private Long recordCount;
        private Long tableSize;


        public String getEmoTable() {
            return emoTable;
        }

        public void setEmoTable(String emoTable) {
            this.emoTable = emoTable;
        }

        public Long getRecordCount() {
            return recordCount;
        }

        public void setRecordCount(Long recordCount) {
            this.recordCount = recordCount;
        }

        public Long getTableSize() {
            return tableSize;
        }

        public void setTableSize(Long tableSize) {
            this.tableSize = tableSize;
        }

        @Override
        public String toString() {
            return "Stat{" +
                    "emoTable=" + emoTable +
                    ", recordCount=" + recordCount +
                    ", tableSize=" + tableSize +
                    '}';
        }
    }
}
