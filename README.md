# Lab assignment Distributed Systems 2021
## Group
Implement a Key-Value Store Using AWS S3 2

To setup the project, you have to add AWS S3 credential info at lines 14-16 in kvstore/s3.py

Please note that when running equal scripts, one may need to run `clear_s3.py` to clear the data on S3, or handle the exception when a key-value pair is attempted to be created with a key that was already used previously.

## Students
- Gabor Banyai (2696642)
- Folkert van Verseveld (2717019)
- Basel Aslan (2698015)
- Auke Schuringa (2585488)
- Asror Akbarkhodjaev (2634228)

# Requirements
## Mandatory Requirements:
1. Setup a benchmark to measure the performance of uploading small key-value pairs (32B - 1KB) to S3.
2. Implement a KV store that uses AWS S3 as a persistent log to persist data. 
3. Design and implement experiments to demostrate the performance and fault tolerance characteristics of your system:
    - The program should successfully upload both DB and persistent log to s3;
    - The program should successfully update existing DB and persistent log to s3;
    - The program should operate successfully if the program crashes:
        - Once program is operational, interrupted log uploads should be uploaded;
    - Latency measurements should be used to compare it with the benchmark performance measures.

## The files and directories
- `example/`: Small example scripts for using the key-value store system.
- `experiments/`: The implementation of perfromance and fault-talerance experiments.
- `kvstore/`: The implementation of the key-value store system.
- `obsolete/`: Files that were used previously, but which have not been used in the final result.
- `clear_s3.py`: Clear all data from AWS S3. Run with `python3 clear_s3.py`

# Experiments

## Performance
To check the how our cache system performs compared to direct upload of kv pairs, 
you should first run benchmark.py and then check out latency.py. 
Give it some time to run, cause it takes 5-10 minutes to get all values.

Procedure:
- First test benchmark
```
python experiments/performance/benchmark.py
```
- Then test chache
```
python experiments/performance/latency.py
```

## Fault-Tolerance
To verify the KV store can recover from crashes, we implemented a crash test experiment.
This test can be executed by running `./crash_test`

See the crash test file for more details and how to change experiment configurations.
The crash test data used for the report can be found in `fault_tolerance_logs`.
Note that the script assumes python invokes the python3 environment. This should work automagically when running the virtual environment.
