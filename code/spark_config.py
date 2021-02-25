from pyspark import SparkConf


def setup_spark(app_name):
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName(app_name)
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

    return conf
