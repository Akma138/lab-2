package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.FormatWeatherDate.formatPerHour;

public class SparkTest {

    final String testString1 = "0,20.03.2021 14:40:56.375,area1,5,sensor455_temp";
    final String testString2 = "0,20.03.2021 15:40:56.375,area2,2,sensor455_temp";
    final String testString3 = "0,20.03.2021 15:40:56.375,area1,2,sensor455_temp";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOneLog() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1));
        JavaRDD<Row> result = formatPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals("area1");
        assert rowList.iterator().next().getString(1).equals("20.03.2021 14:00:00.000");
        assert rowList.iterator().next().getLong(3) == 1;
    }

    @Test
    public void testTwoLogsSameTime(){


        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString1));
        JavaRDD<Row> result = formatPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals("area1");
        assert rowList.iterator().next().getString(1).equals("20.03.2021 14:00:00.000");
        assert rowList.iterator().next().getLong(3) == 2;
    }

    @Test
    public void testTwoLogsDifferentTime(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString3));
        JavaRDD<Row> result = formatPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getString(0).equals("area1");
        assert firstRow.getString(1).equals("20.03.2021 14:00:00.000");
        assert firstRow.getLong(3) == 1;

        assert secondRow.getString(0).equals("area1");
        assert secondRow.getString(1).equals("20.03.2021 15:00:00.000");
        assert secondRow.getLong(3) == 1;
    }

    @Test
    public void testThreeLogs(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString2, testString3));
        JavaRDD<Row> result = formatPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getString(0).equals("area1");
        assert firstRow.getString(1).equals("20.03.2021 14:00:00.000");
        assert firstRow.getLong(3) == 1;

        assert secondRow.getString(0).equals("area2");
        assert secondRow.getString(1).equals("20.03.2021 15:00:00.000");
        assert secondRow.getLong(3) == 1;

        assert thirdRow.getString(0).equals("area1");
        assert thirdRow.getString(1).equals("20.03.2021 15:00:00.000");
        assert thirdRow.getLong(3) == 1;
    }

}
