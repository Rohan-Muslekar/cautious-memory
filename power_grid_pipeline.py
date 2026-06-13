import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, avg, col, lit, count, max as spark_max, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, TimestampType
)

STREAM_DIRECTORY = '/tmp/power_stream'
SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
ZONE_MAPPING_PATH = os.path.join(SCRIPT_DIRECTORY, 'data', 'zone_mapping.csv')

SLIDING_WINDOW_DURATION = '5 minutes'
SLIDING_WINDOW_STEP = '1 minute'
WATERMARK_THRESHOLD = '10 minutes'


def create_spark_session():
    return (
        SparkSession.builder
        .appName('SmartPowerGridMonitoring')
        .config('spark.sql.shuffle.partitions', '4')
        .getOrCreate()
    )


def build_pipeline(spark):
    power_reading_schema = StructType([
        StructField('event_time', TimestampType(), True),
        StructField('meter_id', StringType(), True),
        StructField('global_active_power', FloatType(), True),
    ])

    zone_mapping = spark.read.option('header', True).csv(ZONE_MAPPING_PATH)

    power_stream = (
        spark.readStream
        .schema(power_reading_schema)
        .option('header', True)
        .option('maxFilesPerTrigger', 1)
        .csv(STREAM_DIRECTORY)
    )

    # enrich each reading with its zone metadata before aggregation
    enriched_stream = power_stream.join(zone_mapping, on='meter_id', how='inner')

    windowed_consumption_per_zone = (
        enriched_stream
        .withWatermark('event_time', WATERMARK_THRESHOLD)
        .groupBy(
            window(col('event_time'), SLIDING_WINDOW_DURATION, SLIDING_WINDOW_STEP),
            col('zone_id'),
            col('zone_type')
        )
        .agg(
            spark_round(avg('global_active_power'), 3).alias('avg_consumption_kw'),
            spark_round(spark_max('global_active_power'), 3).alias('peak_consumption_kw'),
            count('*').alias('reading_count')
        )
    )

    return windowed_consumption_per_zone


def detect_grid_anomalies(micro_batch_dataframe, batch_id):
    """Called once per micro-batch. Compares residential zone consumption
    against the industrial average and prints alerts."""
    if micro_batch_dataframe.isEmpty():
        return

    industrial_average_result = (
        micro_batch_dataframe
        .filter(col('zone_type') == 'industrial')
        .agg(spark_round(avg('avg_consumption_kw'), 3).alias('avg'))
        .collect()
    )

    industrial_average_kw = industrial_average_result[0]['avg']
    if industrial_average_kw is None:
        return

    anomalous_residential_zones = (
        micro_batch_dataframe
        .filter(
            (col('zone_type') == 'residential')
            & (col('avg_consumption_kw') > industrial_average_kw)
        )
        .withColumn('industrial_avg_kw', lit(float(industrial_average_kw)))
        .withColumn('status', lit('GRID_ANOMALY'))
        .select(
            'zone_id',
            col('window.start').alias('window_start'),
            col('window.end').alias('window_end'),
            'avg_consumption_kw',
            'industrial_avg_kw',
            'reading_count',
            'status'
        )
    )

    alert_count = anomalous_residential_zones.count()
    if alert_count > 0:
        print(f'\n{"=" * 62}')
        print(f'  GRID ANOMALY ALERT  |  Batch {batch_id}')
        print(f'  Industrial baseline avg: {industrial_average_kw} kW')
        print(f'  Residential zones above threshold: {alert_count}')
        print(f'{"=" * 62}')
        anomalous_residential_zones.show(truncate=False)
    else:
        print(f'[batch {batch_id}] All zones nominal '
              f'(industrial avg: {industrial_average_kw} kW)')


def main():
    if not os.path.exists(ZONE_MAPPING_PATH):
        print(f'Error: {ZONE_MAPPING_PATH} not found.')
        print('Run this script from the project root directory.')
        sys.exit(1)

    os.makedirs(STREAM_DIRECTORY, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel('ERROR')

    print('\nSmart Power Grid Monitoring Pipeline')
    print('=' * 40)
    print(f'Stream source:  {STREAM_DIRECTORY}')
    print(f'Zone mapping:   {ZONE_MAPPING_PATH}')
    print(f'Window:         {SLIDING_WINDOW_DURATION}, step {SLIDING_WINDOW_STEP}')
    print(f'Watermark:      {WATERMARK_THRESHOLD}')
    print('-' * 40)
    print('Waiting for data...\n')

    windowed_consumption = build_pipeline(spark)

    query = (
        windowed_consumption
        .writeStream
        .outputMode('complete')
        .foreachBatch(detect_grid_anomalies)
        .start()
    )

    query.awaitTermination()


if __name__ == '__main__':
    main()
