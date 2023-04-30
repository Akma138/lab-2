package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;

import static java.time.temporal.ChronoField.YEAR;

@AllArgsConstructor
@Slf4j
public class FormatWeatherDate {

    // Формат времени логов - н-р, '20.03.2021 14:02:15.411'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("dd.MM.yyyy HH:mm:ss.SSS")
            .parseDefaulting(YEAR, 2021)
            .toFormatter();

    public static JavaRDD<Row> formatPerHour(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        Dataset<WeatherInfo> weatherInfoDataset = words.map(s -> {
                    String[] logFields = s.split(",");
                    LocalDateTime date = LocalDateTime.parse(logFields[1], formatter);
                    LocalDateTime newDate = date.withMinute(0).withSecond(0).withNano(0);
                    String dateString = newDate.format(formatter);
                    String area = logFields[2];
                    String sensorData = logFields[4];
                    return new WeatherInfo(dateString, area, sensorData);
                }, Encoders.bean(WeatherInfo.class))
                .coalesce(1);

        // Группирует по значениям часа и уровня логирования
        Dataset<Row> t = weatherInfoDataset
                .toDF("area", "date", "sensorData");
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }
}
