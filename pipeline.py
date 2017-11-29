# Common pipeline operations and utilities.

from pyspark.sql import SparkSession


def pipeline(name):
    def pipeline_runner(pipeline_func):
        def wrapper(*args, **kwargs):
            spark = SparkSession.builder.appName(name).getOrCreate()
            try:
                pipeline_func(spark, *args, **kwargs)
            except Exception as e:
                # Do something on failure.
                raise e
            finally:
                spark.stop()
        return wrapper
    return pipeline_runner
