package amazon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class S3Accessor {

//    private S3FileSystem _s3FileSystem;
//    private JavaSparkContext _sc;
//
//    public S3Accessor(JavaSparkContext sc, String baseURI) throws IOException {
//
//        _sc = sc;
//        _s3FileSystem = new S3FileSystem();
//        Configuration conf = new Configuration();
//
//        _sc.getConf().log().info("attempting to open baseURI: {}", baseURI);
//        _s3FileSystem.initialize(URI.create(baseURI), conf);
//
//        _sc.getConf().log().info("getScheme: {}", _s3FileSystem.getScheme());
//        _sc.getConf().log().info("getUri: {}", _s3FileSystem.getUri());
//
//        _s3FileSystem.setWorkingDirectory(new Path (baseURI));
//        _s3FileSystem.getFileStatus(getWorkingDirectory());
//    }
//
//    public Path getWorkingDirectory () {
//        return _s3FileSystem.getWorkingDirectory();
//    }
//
//    // return directories directly contained in the given directory (no recursive descent)
//    public List<Path> getDirectories (Path directory) throws IOException {
//
//        List<Path> list = new ArrayList<>();
//
//        FileStatus[] statusArray = _s3FileSystem.listStatus(directory);
//        for (FileStatus status : statusArray) {
//            Path path = status.getPath();
//            if (status.isDirectory()) {
//                list.add(status.getPath());
//                _sc.getConf().log().info("adding directory path: {}", path.toString());
//            }
//            else {
//                _sc.getConf().log().warn("ignoring non-directory path: {}", path.toString());
//            }
//        }
//        return list;
//    }
//
//    public String getFileContents (Path directory, String fileName) throws IOException {
//
//        FileStatus status = _s3FileSystem.getFileStatus(directory);
//        if (!status.isDirectory()) {
//            throw new IllegalArgumentException("not a directory: " + directory);
//        }
//
//        Path fPath = new Path (fileName);
//        FileStatus fStatus = _s3FileSystem.getFileStatus(fPath);
//        if (!fStatus.isFile()) {
//            throw new IllegalArgumentException("not a file: " + fileName);
//        }
//
//        FSDataInputStream is = _s3FileSystem.open(fStatus.getPath());
//        ByteArrayOutputStream os = new ByteArrayOutputStream();
//
//        int nSize;
//        byte[] buffer = new byte [16384];
//        while ((nSize = is.read(buffer, 0, buffer.length)) != -1) {
//            os.write (buffer);
//        }
//
//        os.flush();
//        _sc.getConf().log().info("read file: {} size: {}", fileName, nSize);
//        return os.toString();
//    }
}
