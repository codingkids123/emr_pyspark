#!/bin/bash -e
# Sync code to this node.

SRC_FOLDER=$1
BASE_FOLDER=$2
S3_BUCKET=your-bucket

if [ -z "$SRC_FOLDER" ]; then
    echo "Must specify source folder."
    exit 1
fi

if [ -z "$BASE_FOLDER" ]; then
    echo "Must specify base folder."
    exit 1
fi

# Sync code from S3.
aws s3 sync --delete s3://$S3_BUCKET/$BASE_FOLDER/$SRC_FOLDER /home/hadoop/$BASE_FOLDER
chmod u+x /home/hadoop/$BASE_FOLDER/config/sync_node.sh
chmod u+x /home/hadoop/$BASE_FOLDER/config/sync_cluster.sh

# Copy SSH key to access slave nodes.
cp -rf /home/hadoop/$BASE_FOLDER/config/id_rsa* /home/hadoop/.ssh/
chmod 400 /home/hadoop/.ssh/id_rsa

# Install Python dependencies.
sudo /usr/local/bin/pip install -r /home/hadoop/$BASE_FOLDER/requirements.txt