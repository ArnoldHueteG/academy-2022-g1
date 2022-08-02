sudo apt install default-jdk

sudo apt-get install apt-transport-https ca-certificates gnupg

# Add the gcloud CLI distribution URI as a package source
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update and install the gcloud CLI
sudo apt-get update && sudo apt-get install google-cloud-cli

sudo apt install python3

sudo apt install python3-pip

# Install or upgrade bigquery client library
pip install --upgrade google-cloud-bigquery

# Install or upgrade cloud storage client library
pip install --upgrade google-cloud-storage

pip install --upgrade pyspark

# Run gcloud
gcloud init

# gcloud authenticate
gcloud auth login --brief --launch-browser --update-adc