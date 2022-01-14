import os
import time

from kvstore import new_item, get_item, initialize, store_multi_item, store_database
from kvstore.config import config


def main():
    initialize()
    val = b'asror'
    #print(get_item('student1').value)
    #print(get_item('student6').value)
    item = new_item('student1', val)
    item = new_item('test123', os.urandom(32))
    print('manually storing multi item')
    store_multi_item()
    item = new_item('student2', b'folkert')
    print('manually storing multi item')
    store_multi_item()
    print('manually storing database')
    store_database()
    item = new_item('student3', b'auke')
    print('manually storing multi item')
    store_multi_item()
    time.sleep(4)
    item = new_item('student4', b'auke1')
    item = new_item('student5', b'auke2')
    item = new_item('student6', b'auke3')
    print('should store multi item?')
    time.sleep(5)

if __name__ == '__main__':
    main()

