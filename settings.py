# Common settings, e.g. base S3 Bucket, runtime environment (prod or test).
# Settings can be configured through environment variable using or command line arguments

EC2_KEY_NAME = 'your-key-name'
EC2_SUBNET_ID = 'your-subnet-id'
JOBFLOW_ROLE = 'your-jobflow-role'
SERVICE_ROLE = 'your-service-role'
EMR_RELEASE_LABEL = 'emr-5.9.0'

S3_BUCKET = 'your-bucket'
BASE_FOLDER = 'pyspark'
LOG_URI = 's3://%s/logs' % S3_BUCKET
REMOTE_SRC = 's3://%s/%s/' % (S3_BUCKET, BASE_FOLDER)
LOCAL_SRC = '/home/hadoop/%/' % BASE_FOLDER

SPARK_SUBMIT_PACKAGES = []

