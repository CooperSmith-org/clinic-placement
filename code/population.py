from pyspark import SparkContext, HiveContext
from pyspark.sql.functions import dayofweek, month, to_timestamp, countDistinct, year
import pyspark.sql.functions as sf
import geopandas as gpd
from shapely.geometry import Point
from spark_config import *


def read_cdr_data(db_table_name):

    conf = setup_spark('Cluster Population')
    sc = SparkContext(conf=conf)
    sql_context = HiveContext(sc)
    df = sql_context.sql("SELECT * FROM tnm_production."+db_table_name)
    return df


def calculate_average_data(df):

    df_average_data = df.groupBy('cluster_id','lat', 'lon').\
        agg(sf.avg('count').alias('avg')).select('cluster_id', 'lat', 'lon', 'avg')

    return df_average_data


def calculate_population_general(df):

    df_aggregated_data = df.groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").
        alias('count')).select('event_date', 'cluster_id', "count", "lat", "lon")

    df_average_data = calculate_average_data(df_aggregated_data)
    df_aggregated_data.unpersist()
    return df_average_data


def calculate_population_cluster_night_weekends(cdr_data):

    # periods covered 8:00pm 12:00am 4:00am 8:00am
    periods = ['20:00:00', '00:00:00', '04:00:00', '08:00:00']

    # Sunday and Saturday
    days = [1, 7]

    df_aggregated_data = cdr_data.where( (dayofweek(to_timestamp("event_date")).isin(days)) | (cdr_data.period.isin(periods))). \
        groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    df_average_data = calculate_average_data(df_aggregated_data)
    df_aggregated_data.unpersist()

    return df_average_data


def calculate_population_cluster_weekly_night_weekends(cdr_data):

    # periods covered 8:00pm 12:00am 4:00am 8:00am
    periods = ['20:00:00', '00:00:00', '04:00:00', '08:00:00']

    # Sunday and Saturday
    days = [1, 7]

    df_aggregated_data = cdr_data.where( (dayofweek(to_timestamp("event_date")).isin(days)) | (cdr_data.period.isin(periods))). \
        groupBy([sf.weekofyear("event_date").alias("date_by_week"), sf.year("event_date").alias("year"), "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('date_by_week', 'year', 'cluster_id', "count", "lat", "lon")

    df_average_data = calculate_average_data(df_aggregated_data)
    df_aggregated_data.unpersist()

    return df_average_data


def calculate_population_cluster_weekdays_day(df):

    # periods covered 12:00pm 04:00pm
    periods = ['12:00:00', '16:00:00']

    # Monday, Tuesday, Wednesday, Thursday and Friday
    days = [2, 3, 4, 5, 6]

    df_aggregated_data = df.where( (dayofweek(to_timestamp("event_date")).isin(days)) | (df.period.isin(periods))). \
        groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    df_average_data = calculate_average_data(df_aggregated_data)
    df_aggregated_data.unpersist()

    return df_average_data


def calculate_population_cluster_rainy_season(df):

    # November, December, January, February, March, April
    months = [11, 12, 1, 2, 3, 4]

    df_aggregated_data = df.where( month(to_timestamp("event_date")).isin(months) ). \
        groupBy(["event_date", "cluster_id", "lat", "lon"]).agg(countDistinct("subscriber_id").alias('count')). \
        select('event_date', 'cluster_id', "count", "lat", "lon")

    df_average_data = calculate_average_data(df_aggregated_data)
    df_aggregated_data.unpersist()

    return df_average_data


def spark_df_to_geopandas_df_for_points(sdf, output_file):
    df = sdf.toPandas()
    df.to_csv(output_file+'.csv', index=False)
    gdf = gpd.GeoDataFrame(
        df.drop(['lon', 'lat'], axis=1),
        crs={'init': 'epsg:4326'},
        geometry=[Point(xy) for xy in zip(df.lon, df.lat)])
    del df
    return gdf


def generate_shape_file(df_aggregated, output_file):

    gdf = spark_df_to_geopandas_df_for_points(df_aggregated, output_file)
    gdf.to_file(output_file+'.shp', index=False)
    df_aggregated.unpersist()

if __name__ == "__main__":

    # table name
    table_name = "cdr_clustered"

    # Read Data from spark table
    df = read_cdr_data(table_name)

    # # # # Generate Data for all data population
    df_agg = calculate_population_general(df)
    generate_shape_file(df_agg, 'population_based_on_all_data_output')
    df_agg.unpersist()

    # Generate Data for night and weekends population
    df_agg = calculate_population_cluster_night_weekends(df)
    generate_shape_file(df_agg, 'population_based_on_night_weekend_output')
    df_agg.unpersist()

    # Generate Data for days and week days population
    df_agg = calculate_population_cluster_weekdays_day(df)
    generate_shape_file(df_agg, 'population_based_on_weekdays_days_output')
    df_agg.unpersist()

    # Generate Data for rainy season population
    df_agg = calculate_population_cluster_rainy_season(df)
    generate_shape_file(df_agg, 'population_based_rainy_season_output')
    df_agg.unpersist()

    # Generate Datafor night and weekends population weekly aggregation
    df_agg = calculate_population_cluster_weekly_night_weekends(df)
    generate_shape_file(df_agg, 'population_based_on_weekly_night_weekend_week_output')
    df_agg.unpersist()