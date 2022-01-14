import threading
import time

from kvstore.config import config
from kvstore.database import store_database
from kvstore.item import new_multi_item


def swap_multi_item():
    """Swap a multi-item for a new one and upload the 'old' multi-item."""
    print('storing multi item')
    multi_item = config.CURRENT_MULTI_ITEM
    config.CURRENT_MULTI_ITEM = new_multi_item()
    if multi_item is not None:
        multi_item.upload()


def _watch_database():
    """Trigger storing the database.

    Using the interval in the configuration, check periodically if it is time
    to store the current database, overwriting the old stored database.
    """
    last_time_local = time.time()
    last_time_online = last_time_local
    while True:
        #print('check database')
        current_time = time.time()
        if current_time - last_time_online > config.DATABASE_DUMP_INTERVAL_ONLINE:
            print(current_time - last_time_online, 'time has passed')
            store_database(local=None)
            last_time_online = current_time
        #if current_time - last_time_local > config.DATABASE_DUMP_INTERVAL_OFFLINE:
        #    store_database()
        #    last_time_local = current_time
        time.sleep(1)


def _watch_multi_item():
    """Trigger the creation and swapping of new multi-items.

    Using the maximum size of a multi-item, this function periodically checks if
    the multi-item is large enough to be swapped for a new multi-item, and be
    uploaded.
    """
    while config.CURRENT_MULTI_ITEM is None:
        time.sleep(0.1)
    while True:
        #print('check multi item', len(config.CURRENT_MULTI_ITEM))
        if len(config.CURRENT_MULTI_ITEM) >= config.MAX_MULTI_ITEM_SIZE:
            #print('multi item size', len(config.CURRENT_MULTI_ITEM))
            swap_multi_item()
        time.sleep(1)


def watcher():
    """Initiate the watchers.

    Returns:
        (Thread, Thread): The threads for the two created watchers.
    """
    thread1 = threading.Thread(target=_watch_database, daemon=True)
    thread2 = threading.Thread(target=_watch_multi_item, daemon=True)
    thread1.start()
    thread2.start()
    return thread1, thread2

