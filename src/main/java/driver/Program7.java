package driver;

import com.bazaarvoice.emodb.common.stash.StandardStashReader;
import com.bazaarvoice.emodb.common.stash.StashReader;
import com.bazaarvoice.emodb.hadoop.io.EmoFileSystem;
import com.bazaarvoice.emodb.hadoop.io.StashFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Trying out Bill's StandardStashReader.  WORKS.
 */
public class Program7 extends AbstractProgram implements Serializable {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-'T'SSS");
    public static final String NAME = Program7.class.getSimpleName();

    private final JavaSparkContext _sc;

    //private String _outputPath;
    //private String _targetPath;

    private static final Logger _log = Logger.getLogger(Program7.class);
    private final List<String> _results = new ArrayList<>();

    public static void main (String[] args) {
        //checkArgs (args, 1, "usage: " + NAME + "{output path}");
        Program7 program = new Program7(args);
        program.process();
    }

    public Program7(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Program7.class.getSimpleName());
        _sc = new JavaSparkContext(conf);

        //_outputPath = args[0];
        //_targetPath = args[1];

        //_log.info("output path: " + _outputPath);
        //_log.info("target path: " + _targetPath);
    }

    public void process () {
        try {
            StandardStashReader reader = StandardStashReader.getInstance(URI.create("s3://emodb-us-east-1/stash/ci"));
            _log.info("stash latest: " +reader.getLatest());

            Iterator<String> i = reader.listTables();
            while (i.hasNext()) {
                String file = i.next();
                _log.info("stash file: " + file);
            }
        }
        catch (Exception e) {
            _log.error("problem processing ", e);
        }
    }

//    public void process2() {
//
//        try {
//            Configuration hConfig = new Configuration();
//            hConfig.setClass("fs.emostash.impl", StashFileSystem.class, FileSystem.class);
//
//            StashFileSystem sfs = new StashFileSystem();
//            sfs.initialize(new URI("emostash://ci.us/dummy"), hConfig);
//
//            _log.info("stash URI: " +sfs.getUri());
//            _log.info("stash working directory: " + sfs.getWorkingDirectory());
//
//            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = sfs.listFiles(sfs.getWorkingDirectory(), true);
//            while (locatedFileStatusRemoteIterator.hasNext()) {
//                LocatedFileStatus status = locatedFileStatusRemoteIterator.next();
//                _log.info("status: " + status.getPath());
//            }
//        }
//        catch (Exception e) {
//            _log.error("problem processing ", e);
//        }
//    }

}
