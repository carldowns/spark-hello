package driver;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;


import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This version contains some tests with Spark SQL
 */
public class Program14 implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    private static final String TIMESTAMP = DATE_FORMAT.format(new Date());
    private static final Logger log = Logger.getLogger(Program14.class);

    private final Map<String,StatCollector> stats = new ConcurrentHashMap<>();
    private final String inputURI;
    private final String outputURI;
    private final Boolean isDebugMode = true;
    private final static Gson gson = new Gson();

    public static void main (String[] args) {
        Program14 program = new Program14(args[0], args[1]);
        //program.process();
        //program.process2();
        //program.process3();
        //program.process4();
        program.test();
    }

    public Program14(String inputURI, String outputURI) {
        this.inputURI = inputURI;
        this.outputURI = outputURI;
        log.info(format("input URI: ", inputURI));
        log.info(format("output URI: ", outputURI));
    }

    /**
     * WORKS with 1.3.1!
     */
    private void test () {

        logInfo("test");

        //String inputURI = "s3n://bucket/whatever/jsonInputFile/*";
        //String outputURI = "s3n://bucket/whatever/test.parquet";
        String inputURI = "s3n://emodb-us-east-1/stash/ci/2015-04-28-00-00-00/answer-3m_mpro/*";
        String outputURI = "s3n://emodb-etl-us-east-1/cdowns-test/outputs/test.parquet";

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> line = sparkContext.textFile(inputURI);
        SQLContext sqlContext = new SQLContext(sparkContext);
        DataFrame dataFrame = sqlContext.jsonRDD(line);
        dataFrame.saveAsParquetFile(outputURI);
    }

    /**
     * This attempts to write the Json out in a Parquet format.
     * Not tested / finished
     */
    private void process4() {

        logInfo("process4");

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        logInfo("create {lines} RDD from a input URI ");
        JavaRDD<String> line = sparkContext.textFile(inputURI);
        debugRDD("line", sparkContext, line);

        /////////////////
        /////////////////

        logInfo("loading SQL context");
        SQLContext sqlContext = new SQLContext(sparkContext);
        //DataFrame df = sqlContext.jsonFile("examples/src/main/resources/people.json");
        DataFrame df = sqlContext.jsonRDD(line);

        logInfo("data frame rows");
        logInfo(df.showString(100));

        JavaRDD<Row> rows = df.javaRDD();
        List<Row> items = rows.collect();
        for (Row item : items) {
            logInfo("writing row item: ", item.toString());
        }

        String parquetPath = outputURI + "/test.parquet";
        logInfo("output parquet file: ", parquetPath);
        df.saveAsParquetFile(parquetPath);

        //String outputDir = this.getClass().getSimpleName() + "-" + TIMESTAMP;
        //String output = outputURI + "/" + "sample.parquet";
        //logInfo("saving data frame as parquet: ", output);

        // not working for some reason
        //        logInfo("SQL context config");
        //        for (Map.Entry<String,String> entry : sqlContext.conf().settings().entrySet()) {
        //            logInfo("SQL config: ", entry.getKey());
        //        }

        // requires an hdfs context??
        // df.save("./test.parquet", SaveMode.Overwrite);

        // load it back in
        logInfo("reading parquet file: ", parquetPath);
        DataFrame df2 = sqlContext.load(parquetPath);

        JavaRDD<Row> rows2 = df2.javaRDD();
        List<Row> items2 = rows2.collect();
        for (Row item : items2) {
            logInfo("reading row item2: ", item.toString());
        }

        // requires a hive context
        //df.saveAsTable(output+".");

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
            getStat(tableName).recordCount = map2.get(tableName);
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

    private static class StatCollector {
        String tableName;
        Integer recordCount = -1;
        Integer tableSize = -1;
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
