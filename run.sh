# Gcloud Setup ##################
sudo apt-get install apt-transport-https ca-certificates gnupg

# Add the gcloud CLI distribution URI as a package source
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update and install the gcloud CLI
sudo apt-get update && sudo apt-get install google-cloud-cli

# Run gcloud
gcloud init

# gcloud authenticate
gcloud auth login --brief --launch-browser --update-adc

# Python Setup ##################
# Create environment
python3 -m venv .venv

# Activate environment
source .venv/bin/activate

# Install requirements.txt
pip install -r requirements.txt