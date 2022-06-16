# This scripts access to a cloud storage bucket, lists all the files in a determined location

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Construct a Cloud Storage client object.
storage_client = storage.Client()
# Construct a BigQuery client object.
client = bigquery.Client()

def dataset_exists(dataset_id):
    """Determines existence of the dataset."""
    # dataset_id = "project.dataset"

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
        return True
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))
        return False

def create_dataset():
    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.cryptocurrency_historical_prices".format(client.project)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def create_table_from_csv(dataset, table_name, uri):
    # Set table_id to the ID of the table to create.
    table_id = f"secret-tide-353414.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("SNo", "INT64"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Symbol", "STRING"),
            bigquery.SchemaField("Date", "STRING"),
            bigquery.SchemaField("High", "FLOAT64"),
            bigquery.SchemaField("Low", "FLOAT64"),
            bigquery.SchemaField("Open", "FLOAT64"),
            bigquery.SchemaField("Close", "FLOAT64"),
            bigquery.SchemaField("Volume", "FLOAT64"),
            bigquery.SchemaField("Marketcap", "FLOAT64"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

bucket_name = 'academy-2022-g1'
prefix = 'coin_'
uri = f"gs://{bucket_name}/{prefix}*"

if not dataset_exists('cryptocurrency_historical_prices'):
    create_dataset()

create_table_from_csv('cryptocurrency_historical_prices', 'crypto_prices', uri)

