gcloud dataproc jobs submit pyspark wordcount.py \
    --cluster=cluster-name \
    --region=region \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar