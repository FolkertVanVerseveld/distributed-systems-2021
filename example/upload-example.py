import os
import boto3
from botocore.client import Config

#install boto3 using 'pip install boto3' (Windows) or 'pip3 install boto3' (macOS/Linux)
#you need python on your machine to use pip

#you can run this code using 'python example.py' or 'python3 example.py'

# We have removed all the sensitive information from our code, 
# so everyone who is interested can simlpy use their own creditials info

ACCESS_KEY_ID = ''
ACCESS_SECRET_KEY = ''
BUCKET_NAME = ''

# getting 256B file to upload
data = os.urandom(256)

# Connection to S3 Service
s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# File upload to S3 bucket
# You can change the key name
s3.Bucket(BUCKET_NAME).put_object(Key='test.bin', Body=data)

print ("Uploaded")
