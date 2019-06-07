# Insight Data Engineering Project #
In this project I want to use global historical, current, and predicted weather data in to create a pipeline to determine regions that can have a potential flooding, wildfire, or a large snow.
## Data ##
[NOAA Global Historical Climatology Network Daily (GHCN-D)](https://registry.opendata.aws/noaa-ghcn/)  
[Atmospheric Models from Météo-France](https://registry.opendata.aws/meteo-france-models/)
## Challenge:
The NOAA historical data are in text format. They update every day and represent data of a point, eg station. This data will be used to find average past 30 or 50 climate data.

The atmospheric data are in GRIB2 format. They update every 6 hours and predicts 114 hours from now. These data will be compared to average data in order to determine hazardous weather conditions.

## Engineering Challenges:
Make a scalable system that helps user can query historical data with station name, region, country, or distance from a given point.
Stream atmospheric and historical data as soon as update are available.
Process and present as soon as new data published into S3.

## Technology stack:
* S3
* AWS
* Spark
* Kafka
* Airflow
* PosgreSQL or PostGIS
* Java
* Spring
* Hibernate
* MapBox