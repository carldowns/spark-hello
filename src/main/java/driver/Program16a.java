package driver;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This version puts the whole thing together.  Has an optimization to use less
 * memory on the collect() phase.
 *
 * TODO: not tested
 */
public class Program16a implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    private static final String TIMESTAMP = DATE_FORMAT.format(new Date());
    private static final Logger log = Logger.getLogger(Program16a.class);

    private final Map<String,StatCollector> stats = new ConcurrentHashMap<>();
    private final static Gson gson = new Gson();
    private final static Type stringObjectMap = new TypeToken<Map<String, Object>>(){}.getType();
    private final static ProgramArgs programArgs = new ProgramArgs();

    private final static String TABLE_INTRINSIC = "~table";
    private final static String RECORD_SIZE_INTRINSIC = "~recordSize";

    private static class ProgramArgs {
        @Parameter (names={"-inputURI","-i"}, description="root URI where stash files are located")
        String inputURI;

        @Parameter (names={"-outputURI", "-o"}, description="root URI where results files should be written")
        String outputURI;

        @Parameter (names="-akkaFrameSize",
                description="Increase this if your tasks need to send back " +
                "large results to the driver (e.g. using collect() on a large dataset).")
        String akkaFrameSize = "10";

        @Parameter (names={"-partitions"}, description="partitions used for collect()")
        Integer partitions=10;

        @Parameter (names="-maxResultSize", description="Defines max size of results cache")
        String maxResultSize = "2g";

        @Parameter(names = "-debug", description = "Debug mode")
        private boolean debug = false;

        public String toString() {
            return "programArgs " +
                    " inputURI=" + inputURI +
                    " outputURI=" + outputURI +
                    " maxResultSize=" + maxResultSize +
                    " akkaFrameSize=" + akkaFrameSize +
                    " debug=" + debug;
        }
    }

    public static void main (String[] args) {

        JCommander jc = new JCommander(programArgs, args);
        Program16a program = new Program16a();
        program.process();
    }

    public Program16a() {
    }

    private StatCollector getStat (String tableName) {
        StatCollector collector = stats.get(tableName);
        if (collector == null) {
            synchronized (stats) {
                collector = new StatCollector();
                collector.tableName = tableName;
                stats.put(tableName, collector);
                logInfo("created stat: ", tableName);
            }
        }
        return collector;
    }

    private static class StatCollector implements Serializable {
        String tableName;
        Long recordCount = -1L;
        Long tableSize = -1L;
    }

    private void outputStats(String marker, JavaSparkContext sparkContext) {

        if (programArgs.outputURI == null) {
            return;
        }

        List<String> results = new ArrayList<>();
        Gson gson = new Gson();
        for (StatCollector collector : stats.values()) {
            results.add(gson.toJson(collector));
        }

        JavaRDD<String> distResults = sparkContext.parallelize(results);
        String fileName = this.getClass().getSimpleName() + "-" + TIMESTAMP;

        String output = programArgs.outputURI + "/" + fileName + "/" + marker;
        logInfo("output json to : ", output);
        distResults.saveAsTextFile(output);
    }



    private void debugRDD(String marker, JavaSparkContext sparkContext, JavaPairRDD<?, ?> rdd) {

        if (!programArgs.debug || programArgs.outputURI == null) {
            return;
        }

        String outputDir = this.getClass().getSimpleName() + "-" + TIMESTAMP;
        String output = programArgs.outputURI + "/" + outputDir + "/" + marker;
        logInfo("output json to : ", output);
        rdd.saveAsTextFile(output);
    }

    private void debugRDD(String marker, JavaSparkContext sparkContext, JavaRDD<?> rdd) {

        if (!programArgs.debug || programArgs.outputURI == null) {
            return;
        }

        String outputDir = this.getClass().getSimpleName() + "-" + TIMESTAMP;
        String output = programArgs.outputURI + "/" + outputDir + "/" + marker;
        logInfo("output json to : ", output);
        rdd.saveAsTextFile(output);
    }

    private String format (String... args) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("============= ");
        for (String arg : args) {
            stringBuilder.append(arg);
        }
        return stringBuilder.toString();
    }

    private void logInfo(String... args) {
        log.info(format(args));
    }

        private void process() {

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());

        logInfo(programArgs.toString());
        sparkConf.set("spark.driver.maxResultSize", programArgs.maxResultSize);
        sparkConf.set("spark.akka.frameSize", programArgs.akkaFrameSize);
        sparkConf.validateSettings();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // build one RDD of ALL stash files within the scope of the URI
        // The URI can specify some or all of the emo table files.

        logInfo("create {jsonRecord} RDD from a input URI ");
        JavaRDD<String> jsonRecord = sparkContext.textFile(programArgs.inputURI);
        debugRDD("jsonRecord", sparkContext, jsonRecord);

        // Build a javaMap from the raw JSon.

        logInfo("convert {jsonRecord} --> {javaMap}");
        JavaRDD<Map> javaMap = jsonRecord.map(new Function<String, Map>() {
            @Override
            public Map call(String v1) throws Exception {
                try {
                    // Compute a recordSize intrinsic and add to the map
                    // TODO: compute and persist a checksum intrinsic too!
                    Integer recordSize = v1.length();
                    Map<String,Object> map = gson.fromJson(v1, stringObjectMap);
                    map.put(RECORD_SIZE_INTRINSIC, recordSize);
                    return map;
                }
                catch (Exception e) {
                    logInfo("convert {jsonRecord} --> {javaMap}", e.getMessage());
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("error", e.getMessage());
                    return map;
                }
            }
        });
        debugRDD("javaMap", sparkContext, javaMap);

        logInfo("convert {javaMap} --> {tableName:javaMap}");
        JavaPairRDD<String, Map> tableNameJavaMap = javaMap.mapToPair(new PairFunction<Map, String, Map>() {
            @Override
            public Tuple2<String, Map> call(Map o) throws Exception {
                String tableName = (String) o.get(TABLE_INTRINSIC);
                if (tableName == null) tableName = "unknown";
                return new Tuple2<>(tableName, o);
            }
        });
        debugRDD("tableNameJavaMap", sparkContext, tableNameJavaMap);

        // Calculate total table record count

        // TODO: may be able to use aggregate more efficiently

        logInfo("convert {tableName:javaMap} --> {tableName:1}");
        JavaPairRDD<String, Long> tableName1Count = tableNameJavaMap.mapToPair(new PairFunction<Tuple2<String, Map>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Map> stringMapTuple2) throws Exception {
                return new Tuple2<>(stringMapTuple2._1, 1L);
            }
        });
        debugRDD("tableName1", sparkContext, tableName1Count);

        logInfo("reduce by key {tableName:1} --> {tableName:recordCount}");
        JavaPairRDD<String,Long> tableNameRecordCount = tableName1Count.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        debugRDD("tableNameRecordCount", sparkContext, tableNameRecordCount);

        // Calculate total table size

        logInfo("convert {tableName:javaMap} --> {tableName:singleRecordSize}");
        JavaPairRDD<String, Long> tableNameRecordSize = tableNameJavaMap.mapToPair(new PairFunction<Tuple2<String, Map>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Map> stringMapTuple2) throws Exception {
                try {
                    Integer recordSize = (Integer) stringMapTuple2._2.get(RECORD_SIZE_INTRINSIC);
                    return new Tuple2<>(stringMapTuple2._1, (long) recordSize);
                }
                catch (Exception e) {
                    logInfo("convert {tableName:javaMap} --> {tableName:singleRecordSize}", e.getMessage());
                }
                return new Tuple2<>(stringMapTuple2._1, 0L);
            }
        });
        debugRDD("tableNameRecordSize", sparkContext, tableNameRecordSize);

        logInfo("reduce by key {tableName:1} --> {tableName:recordCount}");
        JavaPairRDD<String,Long> tableNameTableSize = tableNameRecordSize.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        debugRDD("tableNameTableSize", sparkContext, tableNameTableSize);

        // chunk the return set down alphabetically using 'starts with' so it will fit on memory.
        // relying on the knowledge that emo tables are always lower case

        for (char character = 'a'; character <= 'z'; character++) {
            final String target = String.valueOf(character);

            JavaPairRDD<String,Long> recordCountSubset = tableNameRecordCount.filter(new Function<Tuple2<String, Long>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                    return tuple._1.startsWith(target);
                }
            });
            debugRDD("recordCountSubset", sparkContext, recordCountSubset);

            logInfo("collecting tableNameRecordCount --> stats: ", target);
            for (Tuple2<String,Long> tuple : recordCountSubset.collect()) {
                StatCollector collector = getStat (tuple._1);
                collector.recordCount = tuple._2();
            }
        }

        // chunk the return set down alphabetically using 'starts with' so it will fit on memory.
        // relying on the knowledge that emo tables are always lower case

        for (char character = 'a'; character <= 'z'; character++) {
            final String target = String.valueOf(character);

            JavaPairRDD<String,Long> tableSizeSubset = tableNameTableSize.filter(new Function<Tuple2<String, Long>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                    return tuple._1.startsWith(target);
                }
            });
            debugRDD("tableSizeSubset", sparkContext, tableSizeSubset);

            logInfo("collecting tableNameTableSize --> stats: " + target);
            for (Tuple2<String,Long> tuple : tableSizeSubset.collect()) {
                StatCollector collector = getStat (tuple._1);
                collector.tableSize = tuple._2();
            }
        }

        // Convert statistics to and RDD, persist it

        logInfo("writing --> stash-stats");
        outputStats("stash-stats", sparkContext);
    }

}
