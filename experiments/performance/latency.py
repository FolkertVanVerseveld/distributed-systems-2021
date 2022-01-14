import os
import time
from botocore.client import Config

from kvstore import new_item, initialize, store_multi_item, store_database
from kvstore.s3 import delete_file, list_files

# you can run this code using 'python experiments/performance/latency.py' 
# or 'python3 experiments/performance/latency.py'

# watcher should be switched off for that experiment

# uploading latency of 32B, 64B, 128B, 256B, 512B, 1024B kv-pairs to s3:
# - 1 pair, 5 pairs, 10 pairs, 25 pairs, 50 pairs, 100 pairs

def clear_bucket():
    for filename in list_files():
        delete_file(filename)

def upload(value, size, i):
    start_time = time.time()
    
    initialize()
    for x in range(i):
        key = 'item-'+str(size)+'-'+str(i)+'-'+str(x)
        item = new_item(key, value)
    store_multi_item()
    store_database()
    
    upload_time = time.time() - start_time

    clear_bucket()
    return upload_time

def main():
    print ("{:<8} {:<20} {:<20} {:<20} {:<20} {:<20} {:<20}".format('time','1pair','5pairs','10pairs','25pairs','50pairs','100pairs'))
    
    for x in range(6): 
        size = 32 * (2 ** x)
        data = os.urandom(size)
        
        ttl_time = ["total"]
        avg_time = ["average"]
        
        tmp = upload(data, size, 1)
        ttl_time.append(str(tmp))
        avg_time.append(str(tmp))
        
        tmp = upload(data, size, 5)
        ttl_time.append(str(tmp))
        avg_time.append(str(tmp/5))
        
        tmp = upload(data, size, 10)
        ttl_time.append(str(tmp))
        avg_time.append(str(tmp/10))
        
        tmp = upload(data, size, 25)
        ttl_time.append(str(tmp))
        avg_time.append(str(tmp/25))
        
        tmp = upload(data, size, 50)
        ttl_time.append(str(tmp))
        avg_time.append(str(tmp/50))
        
        tmp = upload(data, size, 100)
        ttl_time.append(str(tmp))
        avg_time.append(str(tmp/100))
        
        print(str(size)+"B upload time")
        print ("{:<8} {:<20} {:<20} {:<20} {:<20} {:<20} {:<20}".format( ttl_time[0],ttl_time[1],ttl_time[2],ttl_time[3],ttl_time[4],ttl_time[5],ttl_time[6]))
        print ("{:<8} {:<20} {:<20} {:<20} {:<20} {:<20} {:<20}".format(avg_time[0],avg_time[1],avg_time[2],avg_time[3],avg_time[4],avg_time[5],avg_time[6]))
    
if __name__ == '__main__':
    main()
