echo 'Crypto historical data pyspark job'
echo ''
echo 'Choose the execution mode:'
echo '[0] Run in local'
echo '[1] Run in cluster'
read LOCAL_OR_CLUSTER

if [ ${LOCAL_OR_CLUSTER} == 0 ]; then
    echo 'Running job in local machine'
    python3 dataproc/bin/gcs_to_bq.py '/home/luz.plaja/projects/academy-2022-g1/input' '/tmp/spark_output/datacsv'
else
    echo 'Running job in cluster'
    bash dataproc/scripts/run_cluster.sh
fi


