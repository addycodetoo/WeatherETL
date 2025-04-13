import requests
import sys
import os
import json
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
from utils.helpers import load_config

def fetch_weather_data(api_url, api_key, cities):
    raw_data = []
    for city in cities:
        params = {'q': city, 'appid': api_key, 'units': 'metric'}
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            raw_data.append(response.json())
        else:
            print(f"Failed to fetch data for {city}: {response.status_code}")
    return raw_data

def main():
    spark = SparkSession.builder \
        .appName("WeatherETL") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    config = load_config("config/config.yml")
    api_url = config['api']['base_url']
    api_key = config['api']['api_key']
    cities = ["London", "New York", "Tokyo", "Paris", "Sydney"]

    raw_data = fetch_weather_data(api_url, api_key, cities)
    if not raw_data:
        print("No data fetched. Exiting ETL process.")
        spark.stop()
        return

    json_strings = [json.dumps(record) for record in raw_data]
    rdd = spark.sparkContext.parallelize(json_strings)

    # ✅ Schema Definition
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("weather", ArrayType(
            StructType([
                StructField("id", LongType(), True),
                StructField("main", StringType(), True),
                StructField("description", StringType(), True),
                StructField("icon", StringType(), True),
            ])
        ), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("pressure", LongType(), True),
            StructField("humidity", LongType(), True),
        ]), True),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True),
            StructField("deg", LongType(), True),
        ]), True),
        StructField("dt", LongType(), True)
    ])

    raw_df = spark.read.schema(schema).json(rdd)
    raw_df.printSchema()
    raw_df.show(truncate=False)

    # Cleanup output directory before writing
    output_dir = Path("C:/weather_etl_poc/data/pre_transform")
    if output_dir.exists():
        shutil.rmtree(output_dir)

    raw_df.repartition(1).write.json(
        path="C:/weather_etl_poc/data/pre_transform",
        mode="overwrite"
    )

    transformed_df = raw_df.select(
        col('name').alias('location'),
        col('weather')[0]['description'].alias('weather')
    )

    # Cleanup for transformed data directory
    transformed_dir = Path("C:/weather_etl_poc/data/transformed")
    if transformed_dir.exists():
        shutil.rmtree(transformed_dir)

    transformed_df.repartition(1).write.csv(
        path="C:/weather_etl_poc/data/transformed",
        header=True,
        mode="overwrite"
    )

    print("✅ ETL process completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()










# import requests
# import sys
# import os
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

# # Append project directories to the system path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
# from utils.helpers import load_config

# def fetch_weather_data(api_url, api_key, cities):
#     """
#     Fetches weather data from the OpenWeather API for each city provided.
#     Returns a list of JSON objects.
#     """
#     raw_data = []
#     for city in cities:
#         params = {'q': city, 'appid': api_key, 'units': 'metric'}
#         response = requests.get(api_url, params=params)
#         if response.status_code == 200:
#             city_data = response.json()
#             raw_data.append(city_data)
#         else:
#             print(f"Failed to fetch data for {city}: {response.status_code}")
#     return raw_data

# def main():
#     # Initialize Spark Session
#     spark = SparkSession.builder \
#         .appName("WeatherETL") \
#         .config("spark.executor.memory", "4g") \
#         .config("spark.driver.memory", "4g") \
#         .getOrCreate()

#     # Load API configuration (base URL and API key)
#     config = load_config("config/config.yml")
#     api_url = config['api']['base_url']        # e.g. "http://api.openweathermap.org/data/2.5/weather"
#     api_key = config['api']['api_key']           # Insert your API key here
#     cities = ["London", "New York", "Tokyo", "Paris", "Sydney"]

#     # Fetch the weather data from the API
#     raw_data = fetch_weather_data(api_url, api_key, cities)
#     if not raw_data:
#         print("No data fetched. Exiting ETL process.")
#         spark.stop()
#         return

#     # Convert each record into a JSON string (one JSON object per line)
#     json_strings = [json.dumps(record) for record in raw_data]
#     rdd = spark.sparkContext.parallelize(json_strings)

#     # Option 1: Define a schema to help Spark avoid inference issues.
#     # You may include only the fields you need, or add extra fields as necessary.
#     schema = StructType([
#         StructField("coord", StructType([
#             StructField("lon", DoubleType(), True),
#             StructField("lat", DoubleType(), True)
#         ]), True),
#         StructField("weather", ArrayType(StructType([
#             StructField("id", LongType(), True),
#             StructField("main", StringType(), True),
#             StructField("description", StringType(), True),
#             StructField("icon", StringType(), True)
#         ])), True),
#         StructField("base", StringType(), True),
#         StructField("main", StructType([
#             StructField("temp", DoubleType(), True),
#             StructField("feels_like", DoubleType(), True),
#             StructField("temp_min", DoubleType(), True),
#             StructField("temp_max", DoubleType(), True),
#             StructField("pressure", LongType(), True),
#             StructField("humidity", LongType(), True),
#             StructField("sea_level", LongType(), True),
#             StructField("grnd_level", LongType(), True)
#         ]), True),
#         StructField("visibility", LongType(), True),
#         StructField("wind", StructType([
#             StructField("speed", DoubleType(), True),
#             StructField("deg", LongType(), True),
#             StructField("gust", DoubleType(), True)
#         ]), True),
#         StructField("rain", StructType([
#             StructField("1h", DoubleType(), True)
#         ]), True),
#         StructField("clouds", StructType([
#             StructField("all", LongType(), True)
#         ]), True),
#         StructField("dt", LongType(), True),
#         StructField("sys", StructType([
#             StructField("type", LongType(), True),
#             StructField("id", LongType(), True),
#             StructField("country", StringType(), True),
#             StructField("sunrise", LongType(), True),
#             StructField("sunset", LongType(), True)
#         ]), True),
#         StructField("timezone", LongType(), True),
#         StructField("id", LongType(), True),
#         StructField("name", StringType(), True),
#         StructField("cod", LongType(), True)
#     ])

#     # Read the JSON strings into a DataFrame using the defined schema.
#     raw_df = spark.read.schema(schema).json(rdd)

#     # Verify that DataFrame contains data.
#     raw_df.printSchema()
#     raw_df.show(truncate=False)

#     # Save the complete raw JSON data to pre_transform folder (use file:// URI scheme)
#     raw_df.repartition(1).write.json(
#         path="file:///C:/weather_etl_poc/data/pre_transform",
#         mode="overwrite"
#     )

#     # Transform the data: Extract only location and weather description.
#     transformed_df = raw_df.select(
#         col('name').alias('location'),
#         col('weather')[0]['description'].alias('weather')
#     )

#     transformed_df.repartition(1).write.csv(
#         path="file:///C:/weather_etl_poc/data/transformed",
#         header=True,
#         mode="overwrite"
#     )

#     print("✅ ETL process completed successfully.")
#     spark.stop()

# if __name__ == "__main__":
#     main()












# import requests
# import sys
# import os

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_unixtime
# from utils.helpers import load_config

# def main():
#     # Initialize Spark Session
#     spark = SparkSession.builder \
#         .appName("WeatherETL") \
#         .getOrCreate()

#     # Load configuration
#     config = load_config("config/config.yml")
#     api_url = config['api']['base_url']
#     api_key = config['api']['api_key']

#     # Sample cities to fetch weather data for
#     cities = ["London", "New York", "Tokyo", "Paris", "Sydney"]

#     # Fetch data from API
#     raw_data = []
#     for city in cities:
#         params = {
#             'q': city,
#             'appid': api_key,
#             'units': 'metric'
#         }
#         response = requests.get(api_url, params=params)
#         if response.status_code == 200:
#             raw_data.append(response.json())

#     # Create DataFrame from raw data
#     raw_df = spark.createDataFrame(raw_data)

#     # Write pre-transform data
#     raw_df.write.csv(
#         path="../data/pre_transform/raw_weather",
#         mode="overwrite",
#         header=True
#     )

#     # Transform data
#     transformed_df = raw_df.select(
#         raw_df['name'].alias('location'),
#         raw_df['main']['temp'].alias('temperature'),
#         from_unixtime(raw_df['dt']).alias('timestamp')
#     )

#     # Write transformed data
#     transformed_df.write.csv(
#         path="../data/transformed/clean_weather",
#         mode="overwrite",
#         header=True
#     )

#     spark.stop()

# if __name__ == "__main__":
#     main()