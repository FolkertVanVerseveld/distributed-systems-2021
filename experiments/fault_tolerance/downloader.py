"""
downloader for fault-tolerance experiment

don't run this directly, see crash_test
"""
import os
import time
import boto3
import sys
import time

from kvstore import get_item, initialize, store_database
from kvstore.s3 import delete_file, list_files
from struct import unpack


def main():
    if len(sys.argv) != 3:
        print(f'usage: python uploader.py file delay')
        sys.exit(1)

    # read from file for verification
    with open(sys.argv[1], 'r') as f:
        numbers = [int(v) for v in f.readlines()]
        print(numbers)

    initialize()
    delay = float(sys.argv[2])

    keys = []

    # download loop
    for i, num in enumerate(numbers):
        keyname = f'dummy{i}'
        item = None

        while item is None:
            try:
                item = get_item(keyname)
            except KeyError:
                pass

            if item is None:
                time.sleep(delay)
            else:
                #print(f'read "{keyname}"')
                keys += [unpack('<H', item.value)[0]]

    print(f'sum: {sum(keys)}')

if __name__ == '__main__':
    main()
