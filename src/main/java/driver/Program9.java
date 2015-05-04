package driver;

import com.bazaarvoice.emodb.common.stash.StandardStashReader;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This example uses old-thinking and was not tested but shows some interesting technique
 *
 */
public class Program9 extends AbstractProgram implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    public static final String NAME = Program9.class.getSimpleName();

    private final JavaSparkContext _sc;
    private String _outputURI;
    private String _inputURI;
    private Integer _limit;

    private static final Logger _log = Logger.getLogger(Program9.class);
    private final List<String> _results = new ArrayList<>();
    Map<String, StatCollector> _collectors = new ConcurrentHashMap<>();

    public static void main (String[] args) {
        Program9 program = new Program9(args);
        program.process();
    }

    public Program9(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Program9.class.getSimpleName());
        _sc = new JavaSparkContext(conf);

        if (args.length < 2) {
            throw new RuntimeException ("usage: driver.Program9 {output URI} {input URI} {limit}");
        }

        _outputURI = args[0];
        _inputURI = args[1];
        _limit = Integer.parseInt(args[2]);
    }

    public void process () {

        try {
            List<String> files = getLatestS3StashFile(_limit);
            debug(files);

            for (String filePath : files) {
                StatCollector c = new StatCollector();
                _collectors.put(filePath, c);

                _log.info("============== building collector for : " + filePath);
                c.fileURI = filePath;

                // build an RDD for each separate EmoTable
                c.lines = _sc.textFile(filePath).cache();

                // start the count operation on the background
                c.countFuture = c.lines.countAsync();
            }

            _log.info("============== polling for completed count futures ");

            long remaining = 0;
            do {
                remaining = 0;
                for (StatCollector c : _collectors.values()) {
                    if (!c.countDone) {
                        if (c.countFuture.isDone()) {
                            c.recordCount = c.countFuture.get();
                            c.countDone = true;
                            _log.info("============== count retrieved for : " + c.fileURI + " : " + c.recordCount);
                        }
                        else {
                            remaining++;
                        }
                    }
                }
                _log.info("============== Jobs remaining : " + remaining);
                Thread.sleep(1000);
            } while (remaining != 0);

            outputResults();
        }

        catch (Exception e) {
            _log.error("unable to process Spark job: ", e);
        }
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


    private static class StatCollector {
        String fileURI;
        transient JavaRDD<String> lines;

        Long recordCount;
        transient Boolean countDone = false;
        transient JavaFutureAction<Long> countFuture;
    }

    private void debug(List<String> strings) {
        for (String string : strings) {
            _log.info(string);
        }
    }

    private List<String> getLatestS3StashFile(int limit) {

        List<String> list = new ArrayList<>();

        try {
            int count = 0;
            StandardStashReader reader = StandardStashReader.getInstance(URI.create(_inputURI));

            String latest = reader.getLatest();
            _log.info("stash latest: " +latest);

            Iterator<String> i = reader.listTables();
            while (i.hasNext() && count++ < limit) {
                String file = i.next();
                list.add(_inputURI + '/' + latest + '/' + file);
            }

            return list;
        }
        catch (Exception e) {
            _log.error("Unable to get stash file list ", e);
            throw new RuntimeException ("Unable to get stash file list");
        }
    }

    private void outputResults () {

        if (_outputURI == null) {
            return;
        }

        Gson gson = new Gson();
        for (StatCollector collector : _collectors.values()) {
            _results.add(gson.toJson(collector));
        }

        JavaRDD<String> distResults = _sc.parallelize(_results);
        String fileName = NAME + "-" + DATE_FORMAT.format(new Date());

        String output = _outputURI + "/" + fileName;
        _log.info("============== output json to : " + output);
        distResults.saveAsTextFile(_outputURI + "/" + fileName);
    }
}
