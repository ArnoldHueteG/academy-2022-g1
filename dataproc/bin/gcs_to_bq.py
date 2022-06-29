#!/usr/bin/env python

import pyspark
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def local():
    """Check arguments to see if the execution will be local or in the cloud"""
    if len(sys.argv) == 4:      # Cluster
        return False
    elif len(sys.argv) == 3: 	# Local
        return True
    else:
        print('### Arguments required ###')
        print('local: <inputUri> <outputDataset> <outputTable>')
        print('cluster: <inputFile> <outputFile>')
        raise Exception()

def create_spark_context():
    if local():
        spark = SparkSession \
            .builder \
            .master('local') \
            .appName('spark-bigquery-demo') \
            .getOrCreate()
    else:
        spark = SparkSession \
            .builder \
            .master('yarn') \
            .appName('spark-bigquery-demo') \
            .getOrCreate()
        spark.conf.set('temporaryGcsBucket','academy-2022-g1')
    return spark

def transformations(sc):
    input = sys.argv[1]

    df = sc.read.csv(
        f'{input}', 
        header=True,
        inferSchema=True
    )

    # Check the schema
    df.printSchema()
    df.withColumn("Date",df.Date.cast('date'))
    # crypto_historical_prices.orderBy(crypto_historical_prices.SNo).collect()

    crypto_historical_prices = df.drop('SNo')

    date_splitted = F.split(crypto_historical_prices['Date'], ' ')

    crypto_historical_prices_1 = crypto_historical_prices.withColumn('NewDate', date_splitted.getItem(0)) \
                                                         .withColumn('Hour', date_splitted.getItem(1))

    # crypto_historical_prices_grouped = crypto_historical_prices.groupBy('Date', 'High')

    # crypto_historical_prices_1.collect()

    # print('Counting disntict hour values:')
    # crypto_historical_prices_1.select(F.countDistinct('Hour')).show()   # They're all the same, so we just drop this column

    crypto_historical_prices_1.select(F.countDistinct('Hour')).show()

    crypto_historical_prices_2 = crypto_historical_prices_1.withColumn('Date', crypto_historical_prices_1['NewDate'])
    crypto_historical_prices_3 = crypto_historical_prices_2.drop('Hour', 'NewDate')

    crypto_historical_prices_4 = crypto_historical_prices_3.sort('Date',F.desc('High'))

    crypto_historical_prices_4.show()

    return crypto_historical_prices_4

def write_output(df):
    if local():
        outputFile = sys.argv[2]
        df.write.csv(outputFile)
    else:
        output_table = f'{sys.argv[2]}.{sys.argv[3]}'      # Dataset.Table
        # Saving the data to BigQuery
        df.write.format('bigquery') \
            .option('table', output_table) \
            .save()

sc = create_spark_context()
df = transformations(sc)
write_output(df)