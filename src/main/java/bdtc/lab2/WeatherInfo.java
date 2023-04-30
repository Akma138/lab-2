package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class WeatherInfo {


    private String date;

    private String area;
    private String sensorData;
}
