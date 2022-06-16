# Create cluster
gcloud dataproc clusters create cluster-academy \
    --region=us-central1 \
    --master-machine-type=n1-standard-2 \
    --worker-machine-type=n1-standard-2 \
    --num-workers=2 \
    --master-boot-disk-size=30GB \
    --num-masters=1 \
    --worker-boot-disk-size=30GB \

# gcloud compute ssh cluster-academy-m

gcloud dataproc jobs submit pyspark wordcount.py \
    --cluster=cluster-name \
    --region=region \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar

# hola