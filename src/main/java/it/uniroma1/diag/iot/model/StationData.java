package it.uniroma1.diag.iot.model;

import java.io.Serializable;
import java.util.Date;

/**
 * POJO object for a StationData single value.
 *
 * @author ichatz@gmail.com
 */
public class StationData implements Serializable {

    private String station_id;

    private String timestamp;

    private Date time;

    private double temperature;

    private double humidity;

    private double wind_direction;

    private double wind_intensity;

    private double rain_height;

    public String getStation_id() {
        return station_id;
    }

    public void setStation_id(String station_id) {
        this.station_id = station_id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public double getWind_direction() {
        return wind_direction;
    }

    public void setWind_direction(double wind_direction) {
        this.wind_direction = wind_direction;
    }

    public double getWind_intensity() {
        return wind_intensity;
    }

    public void setWind_intensity(double wind_intensity) {
        this.wind_intensity = wind_intensity;
    }

    public double getRain_height() {
        return rain_height;
    }

    public void setRain_height(double rain_height) {
        this.rain_height = rain_height;
    }

    @Override
    public String toString() {
        return "StationData{" +
                "station_id='" + station_id + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", time=" + time +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", wind_direction=" + wind_direction +
                ", wind_intensity=" + wind_intensity +
                ", rain_height=" + rain_height +
                '}';
    }
}
