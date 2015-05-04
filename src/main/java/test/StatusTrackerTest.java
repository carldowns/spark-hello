package test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaStatusTrackerDemo.java
 */
public class StatusTrackerTest {


    public static final String APP_NAME = "JavaStatusAPIDemo";

    public static final class IdentityWithDelay<T> implements Function<T, T> {
        @Override
        public T call(T x) throws Exception {
            Thread.sleep(2 * 1000);  // 2 seconds
            return x;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Example of implementing a progress reporter for a simple job.
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(
                new IdentityWithDelay<Integer>());
        JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
        while (!jobFuture.isDone()) {
            Thread.sleep(500);  // 1 second
            List<Integer> jobIds = jobFuture.jobIds();
            if (jobIds.isEmpty()) {
                continue;
            }
            //int currentJobId = jobIds.get(jobIds.size() - 1);
            int currentJobId = jobIds.get(0);
            SparkJobInfo jobInfo = sc.statusTracker().getJobInfo(currentJobId);
            SparkStageInfo stageInfo = sc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
            System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
                    " active, " + stageInfo.numCompletedTasks() + " complete");
        }

        System.out.println("Job results are: " + jobFuture.get());
        sc.stop();
    }
}
