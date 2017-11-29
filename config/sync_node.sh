#!/bin/bash -e
# Sync code to this node.

SRC_FOLDER=$1
S3_BUCKET=your-bucket

if [ -z "$SRC_FOLDER" ]; then
    echo "Must specify source folder."
    exit 1
fi

# Sync code from S3.
aws s3 sync --delete s3://$S3_BUCKET/pyspark/$SRC_FOLDER /home/hadoop/pyspark
chmod u+x /home/hadoop/pyspark/config/sync_node.sh
chmod u+x /home/hadoop/pyspark/config/sync_cluster.sh

# Copy SSH key to access slave nodes.
cp -rf /home/hadoop/pyspark/config/id_rsa* /home/hadoop/.ssh/
chmod 400 /home/hadoop/.ssh/id_rsa

# Install Python dependencies.
sudo /usr/local/bin/pip install -r /home/hadoop/pyspark/requirements.txt