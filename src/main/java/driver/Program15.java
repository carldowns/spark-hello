package driver;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This version puts the whole thing together.
 */
public class Program15 implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    private static final String TIMESTAMP = DATE_FORMAT.format(new Date());
    private static final Logger log = Logger.getLogger(Program15.class);

    private final Map<String,StatCollector> stats = new ConcurrentHashMap<>();
    private final String inputURI;
    private final String outputURI;
    private final Boolean isDebugMode = false;
    private final static Gson gson = new Gson();
    private final static Type stringObjectMap = new TypeToken<Map<String, Object>>(){}.getType();


    private final static String TABLE_INTRINSIC = "~table";
    private final static String RECORD_SIZE_INTRINSIC = "~recordSize";

    public static void main (String[] args) {
        Program15 program = new Program15(args[0], args[1]);
        program.process5();
    }

    public Program15(String inputURI, String outputURI) {
        this.inputURI = inputURI;
        this.outputURI = outputURI;
        log.info(format("input URI: ", inputURI));
        log.info(format("output URI: ", outputURI));
    }

    private void process5() {

        logInfo("process5");

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());

        logInfo("configuration change:","spark.driver.maxResultSize", "2g");
        sparkConf.set("spark.driver.maxResultSize", "2g");
        sparkConf.validateSettings();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // build one RDD of ALL stash files within the scope of the URI
        // The URI can specify some or all of the emo table files.

        logInfo("create {jsonRecord} RDD from a input URI ");
        JavaRDD<String> jsonRecord = sparkContext.textFile(inputURI);
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

        // Collect statistics

        logInfo("collecting tableNameRecordCount --> stats");
        for (Tuple2<String,Long> tuple : tableNameRecordCount.collect()) {
            StatCollector collector = getStat (tuple._1);
            collector.recordCount = tuple._2();
        }

        logInfo("collecting tableNameTableSize --> stats");
        for (Tuple2<String,Long> tuple : tableNameTableSize.collect()) {
            StatCollector collector = getStat (tuple._1);
            collector.tableSize = tuple._2();
        }

        // Convert statistics to and RDD, persist it

        logInfo("writing --> stash-stats");
        outputStats("stash-stats", sparkContext);
    }

    /**
     * This is totally working locally and remotely.
     * reads in splat locations using textFile
     * converts to Map object, maps to tableName
     * calculates number of records per table.
     * Not dependent on preserving or parsing the tableName.
     *
     */
    private void process3() {

        logInfo("process3");

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        logInfo("create {lines} RDD from a input URI ");
        JavaRDD<String> line = sparkContext.textFile(inputURI);
        debugRDD("line", sparkContext, line);

        logInfo("convert {lines} --> {javaMap}");
        JavaRDD<Map> objectMap = line.map(new Function<String, Map>() {
            @Override
            public Map call(String v1) throws Exception {
                try {
                    return gson.fromJson(v1, Map.class);
                } catch (Exception e) {
                    logInfo(e.getMessage());
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("error", e.getMessage());
                    return map;
                }
            }
        });
        debugRDD("objectMap", sparkContext, objectMap);

        logInfo("convert {javaMap} --> {tableName:javaMap}");
        JavaPairRDD<String, Map> tableNameObjectMap = objectMap.mapToPair(new PairFunction<Map, String, Map>() {
            @Override
            public Tuple2<String, Map> call(Map o) throws Exception {
                String tableName = (String) o.get("~table");
                if (tableName == null) tableName = "unknown";
                return new Tuple2<>(tableName, o);
            }
        });
        debugRDD("tableNameObjectMap", sparkContext, tableNameObjectMap);

        logInfo("convert {tableName:javaMap} --> {tableName:1}");
        JavaPairRDD<String, Long> tableName1Count = tableNameObjectMap.mapToPair(new PairFunction<Tuple2<String, Map>, String, Long>() {
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

    }

    /**
     * this proves that the splat works and that we can get Gson conversion of Json to a Java Map.
     */
    private void process2() {
        logInfo("process2");

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        logInfo("create {lines} RDD from a input URI ");
        JavaRDD<String> line = sparkContext.textFile(inputURI);
        debugRDD("line", sparkContext, line);

        logInfo("convert {lines} --> {javaMap}");
        JavaRDD<Map> objectMap = line.map(new Function<String, Map>() {
            @Override
            public Map call(String v1) throws Exception {
                try {
                    return gson.fromJson(v1, Map.class);
                }
                catch (Exception e) {
                    logInfo(e.getMessage());
                    HashMap<String,Object> map = new HashMap<>();
                    map.put("error", e.getMessage());
                    return map;
                }
            }
        });
        debugRDD("objectMap", sparkContext, objectMap);

        logInfo("convert {javaMap} --> {tableName:javaMap}");
        JavaPairRDD<String, Map> tableNameObjectMap = objectMap.mapToPair(new PairFunction<Map, String, Map>() {
            @Override
            public Tuple2<String, Map> call(Map o) throws Exception {
                String tableName = (String)o.get("~table");
                if (tableName == null) tableName = "unknown";
                return new Tuple2<>(tableName, o);
            }
        });
        debugRDD("tableNameObjectMap", sparkContext, tableNameObjectMap);
    }


    /**
     * This was a good attempt but relied on handling the original filename which is a mess.
     */
    private void process () {

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        logInfo("open an RDD pair using whole files (fileName:fileContent}");
        JavaPairRDD<String,String> fileName_fileContent = sparkContext.wholeTextFiles(inputURI);

        logInfo("map {fileName:fileContent} --> {fileName:record}");
        JavaPairRDD<String,String> fileName_record = fileName_fileContent.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>,String,String>() {
            public Iterable<Tuple2<String, String>> call(Tuple2<String, String> fileContentTuple) throws Exception {
                String[] records = fileContentTuple._2.split("\n");
                List<Tuple2<String, String>> recordTuples = new ArrayList<>();

                for (String record : records) {
                    log.info(format ("adding record for ", fileContentTuple._1));
                    recordTuples.add (new Tuple2<>(fileContentTuple._1,record));
                }
                return recordTuples;
            }
        });
        debugRDD("fileName_record", sparkContext, fileName_record);


        logInfo("map {tableName:record} ---> {tableName:1}");
        JavaPairRDD<String,Integer> tableName_1 = fileName_record.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
            public Tuple2<String,Integer> call(Tuple2<String, String> v1) throws Exception {
                return new Tuple2<>(v1._1, 1);
            }
        });
        debugRDD("tableName_1", sparkContext, tableName_1);


        logInfo("reduce by key {tableName:recordCount}");
        JavaPairRDD<String,Integer> tableName_recordCount = tableName_1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        debugRDD("tableName_recordCount", sparkContext, tableName_recordCount);

//        {
//            // FIXME: we have the entire contents of various split files, just compute their size instead of this
//            logInfo("map {tableName:record} ---> {tableName:recordLength}");
//            JavaPairRDD<String,Integer> tableName_recordSize = fileName_record.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
//                public Tuple2<String,Integer> call(Tuple2<String, String> v1) throws Exception {
//                    return new Tuple2<>(v1._1, v1._2.length());
//                }
//            });
//            debugRDD("tableName_recordSize", sparkContext, tableName_recordSize);
//
//
//            logInfo("reduce by key {tableName:tableSize}");
//            JavaPairRDD<String,Integer> tableName_tableSize = tableName_recordSize.reduceByKey(new Function2<Integer, Integer, Integer>() {
//                public Integer call(Integer integer, Integer integer2) throws Exception {
//                    return integer + integer2 + 1; // the extra character corresponds to \n above when file is split
//                }
//            });
//            debugRDD("tableName_tableSize", sparkContext, tableName_tableSize);
//        }

        logInfo("map {fileName:fileContent} --> {fileName:fileSize}");
        JavaPairRDD<String,Integer> fileName_fileSize = fileName_fileContent.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
            public Tuple2<String,Integer> call(Tuple2<String, String> v1) throws Exception {
                return new Tuple2<>(v1._1, v1._2.length());
            }
        });
        debugRDD("fileName_fileSize", sparkContext, fileName_fileSize);

        logInfo("reduce by key {fileName:fileSize}");
        JavaPairRDD<String,Integer> fileName_combinedFileSize = fileName_fileSize.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        debugRDD("fileName_combinedFileSize", sparkContext, fileName_combinedFileSize);


//        log.info(format("collect record counts, update stats"));
//        tableName_recordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                getStat(tuple._1).recordCount = tuple._2;
//            }
//        });
//
//        log.info(format("collect table sizes RDD, update stats"));
//        tableName_tableSize.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                getStat(tuple._1).tableSize = tuple._2;
//            }
//        });

        logInfo("collect record counts, update stats");
        Map<String,Integer> map2 = tableName_recordCount.collectAsMap();
        for (String tableName : map2.keySet()) {
            //getStat(tableName).recordCount = map2.get(tableName);
        }

//        logInfo("collect table sizes RDD, update stats");
//        Map<String,Integer> map3 = tableName_tableSize.collectAsMap();
//        for (String tableName : map3.keySet()) {
//            getStat(tableName).tableSize = map3.get(tableName);
//        }

        outputStats("stats", sparkContext);
    }

    private StatCollector getStat (String tableName) {
        StatCollector collector = stats.get(tableName);
        if (collector == null) {
            synchronized (stats) {
                collector = new StatCollector();
                collector.tableName = tableName;
                stats.put(tableName, collector);
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

        if (outputURI == null) {
            return;
        }

        List<String> results = new ArrayList<>();
        Gson gson = new Gson();
        for (StatCollector collector : stats.values()) {
            results.add(gson.toJson(collector));
        }

        JavaRDD<String> distResults = sparkContext.parallelize(results);
        String fileName = this.getClass().getSimpleName() + "-" + TIMESTAMP;

        String output = outputURI + "/" + fileName + "/" + marker;
        logInfo("output json to : ", output);
        distResults.saveAsTextFile(output);
    }



    private void debugRDD(String marker, JavaSparkContext sparkContext, JavaPairRDD<?, ?> rdd) {

        if (!isDebugMode || outputURI == null) {
            return;
        }

        String outputDir = this.getClass().getSimpleName() + "-" + TIMESTAMP;
        String output = outputURI + "/" + outputDir + "/" + marker;
        logInfo("output json to : ", output);
        rdd.saveAsTextFile(output);
    }

    private void debugRDD(String marker, JavaSparkContext sparkContext, JavaRDD<?> rdd) {

        if (!isDebugMode || outputURI == null) {
            return;
        }

        String outputDir = this.getClass().getSimpleName() + "-" + TIMESTAMP;
        String output = outputURI + "/" + outputDir + "/" + marker;
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
}
