from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.functions import dayofweek, month, to_timestamp, countDistinct
import pyspark.sql.functions as sf

import geopandas as gpd
from shapely.geometry import Point


def read_cdr_data(table_name):

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Cluster Population")
    conf.set('spark.executor.memory', '16g')
    conf.set('spark.driver.memory', '10g')
    conf.set('parquet.enable.summary - metadata', False)
    conf.set('spark.sql.parquet.binaryAsString', True)
    conf.set('spark.sql.parquet.mergeSchema', True)
    conf.set('spark.sql.parquet.compression.codec', 'snappy')
    conf.set('spark.rdd.compress', True)
    conf.set('spark.io.compression.codec', 'snappy')
    conf.set('spark.sql.tungsten.enabled', False)
    conf.set('spark.sql.codegen', False)
    conf.set('spark.sql.unsafe.enabled', False)
    conf.set('spark.yarn.executor.memoryOverhead', 8192)
    conf.set('spark.driver.am.memory', '8G')
    conf.set('spark.yarn.am.memoryOverhead', '8g')
    conf.set('spark.scheduler.mode', 'FAIR')
    conf.set('spark.broadcast.compress', True)
    conf.set('spark.io.compression.codec', 'snappy')
    conf.set('spark.dynamicAllocation.enabled', True)

    # Pandas conversion
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    sc = SparkContext(conf=conf)
    sql_context = HiveContext(sc)

    df = sql_context.sql("SELECT * FROM tnm_production."+table_name)
    return df


def calculate_average_data(aggregated_data):

    average_data = aggregated_data.groupBy('cluster_id','lat', 'lon'). \
        agg(sf.avg('count').alias('avg')).select('cluster_id', 'lat', 'lon', 'avg')

    return average_data


def calculate_population_general(cdr_data):

    aggregated_data = cdr_data.groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    average_data = calculate_average_data(aggregated_data)

    return average_data


def calculate_population_cluster_night_weekends(cdr_data):

    # periods covered 8:00pm 12:00am 4:00am 8:00am
    periods = ['20:00:00', '00:00:00', '04:00:00', '08:00:00']

    # Sunday and Saturday
    days = [1, 7]

    aggregated_data = cdr_data.where( (dayofweek(to_timestamp("event_date")).isin(days)) | (cdr_data.period.isin(periods))). \
        groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    average_data = calculate_average_data(aggregated_data)

    return average_data


def calculate_population_cluster_weekdays_day(cdr_data):

    # periods covered 12:00pm 04:00pm
    periods = ['12:00:00', '16:00:00']

    # Monday, Tuesday, Wednesday, Thursday and Friday
    days = [2, 3, 4, 5, 6]

    aggregated_data = cdr_data.where( (dayofweek(to_timestamp("event_date")).isin(days)) | (cdr_data.period.isin(periods))). \
        groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    average_data = calculate_average_data(aggregated_data)

    return average_data


def calculate_population_cluster_rainy_season(cdr_data):

    # November, December, January, February, March, April
    months = [11, 12, 1, 2, 3, 4]

    aggregated_data = cdr_data.where( month(to_timestamp("event_date")).isin(months) ). \
        groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    average_data = calculate_average_data(aggregated_data)

    return average_data


def spark_df_to_geopandas_df_for_points(sdf):
    df = sdf.toPandas()
    gdf = gpd.GeoDataFrame(
        df.drop(['lon', 'lat'], axis=1),
        crs={'init': 'epsg:4326'},
        geometry=[Point(xy) for xy in zip(df.lon, df.lat)])
    return gdf


if __name__ == "__main__":

    # table name
    table_name = "cdr_clustered"

    # Read Data from spark table
    data = read_cdr_data(table_name)

    # # # # Generate Data for all data population
    agg_data = calculate_population_general(data)

    geospark = spark_df_to_geopandas_df_for_points(agg_data)
    geospark.to_file('population_based_on_all_data_output.shp', index=False)

    # Generate Data for night and weekends population
    agg_data = calculate_population_cluster_night_weekends(data)
    geospark = spark_df_to_geopandas_df_for_points(agg_data)
    geospark.to_file('population_based_on_night_weekend_output.shp', index=False)

    # Generate Data for days and week days population
    agg_data = calculate_population_cluster_weekdays_day(data)
    geospark = spark_df_to_geopandas_df_for_points(agg_data)
    geospark.to_file('population_based_on_weekdays_days_output.shp', index=False)

    # Generate Data for rainy season population
    agg_data = calculate_population_cluster_weekdays_day(data)
    geospark = spark_df_to_geopandas_df_for_points(agg_data)
    geospark.to_file('population_based_rainy_season_output.shp', index=False)

