import os

from kvstore.exceptions import S3FileNotFoundError
from kvstore.s3 import *


def main():
    print(list(S3_BUCKET.objects.all()))
    data = os.urandom(1024)
    print('uploading')
    print(upload_file('testkey', data))
    print('uploading')
    print(upload_file('testkey', data))
    print('downloading')
    print(download_file('testkey') == data)
    print('deleting')
    print(delete_file('testkey'))
    print('uploading')
    print(upload_file('testkey', data))
    print('downloading')
    print(download_file('testkey') == data)
    print('deleting')
    print(delete_file('testkey'))
    print('downloading')
    try:
        print(download_file('testkey') == data)
    except S3FileNotFoundError as e:
        print('File was not found.')
    else:
        raise Exception('File should not have been found.')
    print([o.key for o in S3_BUCKET.objects.all()])

if __name__ == '__main__':
    main()

