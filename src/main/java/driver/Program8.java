package driver;

import com.bazaarvoice.emodb.common.stash.StandardStashReader;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * calculates at present 5 random emo tables' stats from _LATEST stash:
 *
 * spark-submit --class driver.Program8
 * --master local[4] target/spark-hello-1.0-SNAPSHOT.jar
 * file:///Users/carl.downs/work/dev/bv/spark-hello/log
 * s3n://emodb-us-east-1/stash/ci
 *
 */
public class Program8 extends AbstractProgram implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    public static final String NAME = Program8.class.getSimpleName();

    private final JavaSparkContext _sc;
    private String _outputURI;
    private String _inputURI;

    private static final Logger _log = Logger.getLogger(Program8.class);
    private final List<String> _results = new ArrayList<>();
    Map<String, StatCollector> _collectors = new ConcurrentHashMap<>();

    public static void main (String[] args) {
        Program8 program = new Program8(args);
        program.process();
    }

    public Program8(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Program8.class.getSimpleName());
        _sc = new JavaSparkContext(conf);

        // inputURI example: "s3://emodb-us-east-1/stash/ci"
        // outputURI example:

        if (args.length < 2) {
            throw new RuntimeException ("usage: Program8 {output URI} {input URI}");
        }

        _outputURI = args[0];
        _inputURI = args[1];

        _log.info("input URI: " + _inputURI);
        _log.info("output URI: " + _outputURI);
    }

    public void process () {

        List<String> files = getLatestS3StashFile(20000);
        debug(files);

        for (String filePath : files) {
            StatCollector collector = new StatCollector();
            _collectors.put(filePath, collector);

            _log.info("============== building collector for : " + filePath);
            collector.fileURI = filePath;
            collector.lines = _sc.textFile(filePath);
        }

        for (StatCollector collector : _collectors.values()) {
            collector.recordCount = collector.lines.count();
            _log.info("============== recordCount for : " + collector.fileURI + " : " + collector.recordCount);
        }

        outputResults();
    }

    private static class StatCollector {
        String fileURI;
        Long recordCount;
        transient JavaRDD<String> lines;
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
