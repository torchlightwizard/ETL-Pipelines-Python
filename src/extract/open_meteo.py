from src.extract.extract import Extract
from src.connect.file import File

import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd

import logging



class OMFetch (Extract):
    def __init__ (self, path="https://api.open-meteo.com/v1/forecast"):
        params = {
            "latitude": 31.1704,
            "longitude": 72.7097,
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "et0_fao_evapotranspiration", "vapour_pressure_deficit"],
	        "timezone": "Asia/Singapore",
            "past_days": 1,
	        "forecast_days": 1
        }
        self.cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)

        super().__init__(path, None, params, None)



    def get(self):
        try:
            retry_session = retry(self.cache_session, retries = 5, backoff_factor = 0.2)
            openmeteo = openmeteo_requests.Client(session = retry_session)
            responses = openmeteo.weather_api(self.path, params=self.params)
            if not responses:
                raise ValueError("Failed to get response from API.\n")
            self.data = responses[0]

        except Exception as err:
            self.data = {"error": "Faulty Data."}
            logging.exception(f"Function: OMFetch.get(). Error: {err}\n")
        return self
        
            

    def process (self):
        try:
            if hasattr(self.data, "error"):
                raise AttributeError(f"Response is an error.\n")
            
            response = self.data

            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
            hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
            hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
            hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
            hourly_rain = hourly.Variables(5).ValuesAsNumpy()
            hourly_cloud_cover_low = hourly.Variables(6).ValuesAsNumpy()
            hourly_cloud_cover_mid = hourly.Variables(7).ValuesAsNumpy()
            hourly_cloud_cover_high = hourly.Variables(8).ValuesAsNumpy()
            hourly_wind_speed_10m = hourly.Variables(9).ValuesAsNumpy()
            hourly_wind_speed_100m = hourly.Variables(10).ValuesAsNumpy()
            hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
            hourly_wind_direction_100m = hourly.Variables(12).ValuesAsNumpy()
            hourly_wind_gusts_10m = hourly.Variables(13).ValuesAsNumpy()
            hourly_soil_temperature_0_to_7cm = hourly.Variables(14).ValuesAsNumpy()
            hourly_soil_temperature_7_to_28cm = hourly.Variables(15).ValuesAsNumpy()
            hourly_soil_moisture_0_to_7cm = hourly.Variables(16).ValuesAsNumpy()
            hourly_soil_moisture_7_to_28cm = hourly.Variables(17).ValuesAsNumpy()
            hourly_et0_fao_evapotranspiration = hourly.Variables(18).ValuesAsNumpy()
            hourly_vapour_pressure_deficit = hourly.Variables(19).ValuesAsNumpy()

            hourly_data = {"date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = hourly.Interval()),
                inclusive = "left"
            )}

            hourly_data["temperature_2m"] = hourly_temperature_2m
            hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
            hourly_data["dew_point_2m"] = hourly_dew_point_2m
            hourly_data["apparent_temperature"] = hourly_apparent_temperature
            hourly_data["precipitation"] = hourly_precipitation
            hourly_data["rain"] = hourly_rain
            hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
            hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
            hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
            hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
            hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
            hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
            hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
            hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
            hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
            hourly_data["soil_temperature_7_to_28cm"] = hourly_soil_temperature_7_to_28cm
            hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm
            hourly_data["soil_moisture_7_to_28cm"] = hourly_soil_moisture_7_to_28cm
            hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
            hourly_data["vapour_pressure_deficit"] = hourly_vapour_pressure_deficit

            hourly_dataframe = pd.DataFrame(data = hourly_data)

            self.data = {
                "Latitude": f"{response.Latitude()}",
                "Longitude": f"{response.Longitude()}",
                "Elevation": f"{response.Elevation()}",
                "Timezone": f"{response.Timezone()}",
                "Timezone_Abrv": f"{response.TimezoneAbbreviation()}",
                "Timezone_Diff": f"{response.UtcOffsetSeconds()}",
                "DataFrame": hourly_dataframe.to_json()
            }

        except Exception as err:
            self.data = {"error": "Faulty Data."}
            logging.exception(f"Function: OMFetch.process(). Error: {err}\n")
        return self