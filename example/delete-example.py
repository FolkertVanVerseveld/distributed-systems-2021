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

# Connection to S3 Service
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# File upload to S3 bucket
s3.delete_object(Bucket=BUCKET_NAME, Key="bitmoji.png")

print ("Deleted")
