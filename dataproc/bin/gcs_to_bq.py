# #!/usr/bin/env python
# """BigQuery I/O PySpark example."""

# import pyspark
# import sys

# if len(sys.argv) != 3:
#   raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

# inputUri=sys.argv[1]
# outputUri=sys.argv[2]

# sc = pyspark.SparkContext()

# textFile = sc.textFile(inputUri)

# textFile.collect()
# textFile.saveAsTextFile(sys.argv[2])


#____

#!/usr/bin/env python

import pyspark
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

if len(sys.argv) != 3:
  raise Exception("Exactly 3 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]
# outputTable=sys.argv[3]

spark = SparkSession.builder.appName('Spark').getOrCreate()
sc = pyspark.SparkContext()
sc = sc.toDF()

# Read CSV files into a dataframe
files = sc.read.csv(
    f'{inputUri}coin_*.csv', 
    header=True,
    inferSchema=True
)

# Check the schema
files.printSchema()

# crypto_historical_prices.orderBy(crypto_historical_prices.SNo).collect()

crypto_historical_prices = files.drop('SNo')

date_splitted = F.split(crypto_historical_prices['Date'], ' ')

crypto_historical_prices_1 = crypto_historical_prices.withColumn('NewDate', date_splitted.getItem(0)) \
                                                     .withColumn('Hour', date_splitted.getItem(1))

# crypto_historical_prices_grouped = crypto_historical_prices.groupBy('Date', 'High')

# crypto_historical_prices_1.collect()

print('Counting disntict hour values:')
crypto_historical_prices_1.select(F.countDistinct('Hour')).show()   # They're all the same, so we just drop this column

crypto_historical_prices_2 = crypto_historical_prices_1.withColumn('Date', crypto_historical_prices_1['NewDate'])
crypto_historical_prices_3 = crypto_historical_prices_2.drop('Hour', 'NewDate')

crypto_historical_prices_3.show()

# crypto_historical_prices_3.saveAsTextFile(sys.argv[2])

# crypto_historical_prices_3.write.format('bigquery') \
#   .option('table', f'{outputDataset}.{outputTable}') \
#   .save()

crypto_historical_prices_3.saveAsTextFile(sys.argv[2])