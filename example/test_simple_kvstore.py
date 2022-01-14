import os

from kvstore import new_item, get_item, initialize, store_multi_item, store_database
from kvstore.database import database


def main():
    print(database())
    initialize()
    print(database())
    value = b'testvalue'
    item = new_item('testkey1', value)
    print(item)
    item = get_item('testkey1')
    print(item, item.bytes, item.str)
    #item = get_item('badkey')
    store_multi_item()
    print(item, item.bytes, item.str)
    store_database()
    print(database())
    

if __name__ == '__main__':
    main()

