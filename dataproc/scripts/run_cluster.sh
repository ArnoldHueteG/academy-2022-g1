PROJECT=analog-foundry-357900
BUCKET_NAME=academy-2022-g1
CLUSTER=academy-project-luz
REGION=us-central1
DATASET=crypto
TABLE=historical_prices

# Create cluster
gcloud dataproc clusters create ${CLUSTER} \
  --region=${REGION} \
  --single-node
  # --master-machine-type=n1-standard-2 \
  # --worker-machine-type=n1-standard-2 \
  # --num-workers=2 \
  # --master-boot-disk-size=30GB \
  # --num-masters=1 \
  # --worker-boot-disk-size=30GB

# # Create dataset
bq mk crypto

# # Create table with schema
# bq mk -t ${DATASET}.${TABLE}

# Submit job
gcloud dataproc jobs submit pyspark dataproc/bin/gcs_to_bq.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  -- gs://${BUCKET_NAME}/input/coin_*.csv ${DATASET} ${TABLE}

gcloud dataproc clusters delete ${CLUSTER} \
  --region=${REGION}