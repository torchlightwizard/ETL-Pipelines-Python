data_schema = [
    {
        "table_name": "open_meteo_data",
        "columns": {
            "id": "INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
            "date": "BIGINT",
            "temperature_2m": "DOUBLE",
            "relative_humidity_2m": "DOUBLE",
            "dew_point_2m": "DOUBLE",
            "apparent_temperature": "DOUBLE",
            "precipitation": "DOUBLE",
            "rain": "DOUBLE",
            "cloud_cover_low": "DOUBLE",
            "cloud_cover_mid": "DOUBLE",
            "cloud_cover_high": "DOUBLE",
            "wind_speed_10m": "DOUBLE",
            "wind_speed_100m": "DOUBLE",
            "wind_direction_10m": "DOUBLE",
            "wind_direction_100m": "DOUBLE",
            "wind_gusts_10m": "DOUBLE",
            "soil_temperature_0_to_7cm": "DOUBLE",
            "soil_temperature_7_to_28cm": "DOUBLE",
            "soil_moisture_0_to_7cm": "DOUBLE",
            "soil_moisture_7_to_28cm": "DOUBLE",
            "et0_fao_evapotranspiration": "DOUBLE",
            "vapour_pressure_deficit": "DOUBLE",
            "latitude": "DOUBLE",
            "longitude": "DOUBLE",
            "elevation": "DOUBLE"
        }
    }
]

predict_schema = [
    {
        "table_name": "open_meteo_predict",
        "columns": {
            "id": "INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
            "date": "DATE",
            "rain": "TINYINT"
        }
    }
]