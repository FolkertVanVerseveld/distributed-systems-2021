"""
uploader for fault-tolerance experiment

don't run this directly, see crash_test
"""
import os
import time
import boto3
import sys
import time

from kvstore import new_item, get_item, initialize, store_database, store_multi_item
from kvstore.s3 import delete_file, list_files


def main():
    if len(sys.argv) != 3:
        print(f'usage: python uploader.py file delay')
        sys.exit(1)

    # read from file for verification
    with open(sys.argv[1], 'r') as f:
        numbers = [int(v) for v in f.readlines()]
        print(numbers)

    # measure recovery time
    start = time.monotonic()
    fst = True
    initialize()
    delay = float(sys.argv[2])

    # upload loop
    for i, num in enumerate(numbers):
        keyname = f'dummy{i}'
        try:
            # if key is present, skip
            # if created, artificial delay
            new_item(keyname, (num).to_bytes(2, byteorder='little'))
            if fst:
                fst = False
                end = time.monotonic()
                print(f'recover time {end - start} sec')
            time.sleep(delay)
            print(f'create "{keyname}"')
        except KeyError as e:
            pass #print(f'skip {keyname}: {e}')

    print(f'sum: {sum(numbers)}')

    store_multi_item()
    store_database()

if __name__ == '__main__':
    main()
