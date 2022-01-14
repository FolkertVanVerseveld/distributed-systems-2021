import os
import time
import boto3
from botocore.client import Config

# you can run this code using 'python experiments/performance/benchmark.py' 
# or 'python3 experiments/performance/benchmark.py'

# We have removed all the sensitive information from our code, 
# so everyone who is intereseted can simlpy use their own creditials info

ACCESS_KEY_ID = '' 
ACCESS_SECRET_KEY = ''
BUCKET_NAME = ''

# Connection to S3 Service
s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# first file usually has higher latency compared to following ones
s3.Bucket(BUCKET_NAME).put_object(Key='benchmark/test_file', Body=os.urandom(32))

def upload(data, size, i):
    upload_time_total = 0
    for x in range(i): 
        start_time = time.time()
        s3.Bucket(BUCKET_NAME).put_object(Key='benchmark/'+str(size)+'file', Body=data)
        upload_time = time.time() - start_time
        upload_time_total += upload_time
    return upload_time_total

def main():
    print ("{:<8} {:<20} {:<20} {:<20} {:<20} {:<20} {:<20}".format('time','1pair','5pairs','10pairs','25pairs','50pairs','100pairs'))
    for x in range(6): 
        size = 32 * (2 ** x)
        data = os.urandom(size)
        
        ttl_time = ["total"]
        avg_time = ["average"]
        
        tmp = upload(data, size, 1)
        ttl_time.append(tmp)
        avg_time.append(tmp)
        
        tmp = upload(data, size, 5)
        ttl_time.append(tmp)
        avg_time.append(tmp/5)
        
        tmp = upload(data, size, 10)
        ttl_time.append(tmp)
        avg_time.append(tmp/10)
        
        tmp = upload(data, size, 25)
        ttl_time.append(tmp)
        avg_time.append(tmp/25)
        
        tmp = upload(data, size, 50)
        ttl_time.append(tmp)
        avg_time.append(tmp/50)
        
        tmp = upload(data, size, 100)
        ttl_time.append(tmp)
        avg_time.append(tmp/100)
        
        print(str(size)+"B upload time")
        print ("{:<8} {:<20} {:<20} {:<20} {:<20} {:<20} {:<20}".format(ttl_time[0],ttl_time[1],ttl_time[2],ttl_time[3],ttl_time[4],ttl_time[5],ttl_time[6]))
        print ("{:<8} {:<20} {:<20} {:<20} {:<20} {:<20} {:<20}".format(avg_time[0],avg_time[1],avg_time[2],avg_time[3],avg_time[4],avg_time[5],avg_time[6]))
        

if __name__ == '__main__':
    main()
