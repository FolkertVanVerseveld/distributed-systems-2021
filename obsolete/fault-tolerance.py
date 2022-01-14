# Simulate 1 crash during the uploading of 100 pairs 32B kv-pairs to s3

# Simulate 5 crashes during the uploading of 100 pairs 32B kv-pairs to s3

# Simulate 10 crashes during the uploading of 100 pairs 32B kv-pairs to s3

# Simulate 25 crashes during the uploading of 100 pairs 32B kv-pairs to s3

# TODO add more tests
import threading
import time

def test_upload(sequence, delay):
    # uploads keys from sequence one by one with delay
    from kvstore import get_item, new_item
    print(f'test_upload: delay={delay:.2f}')
    my_initialize(watch_t_up)

    for i in range(len(sequence)):
        keyname = f'dummy{i}'

        try:
            get_item(keyname)
            continue
        except KeyError:
            new_item(keyname, (sequence[i]).to_bytes(2, byteorder='little'))
            print(f'create "{keyname}"')

        time.sleep(delay)

    store_database()

def test_upload_counter(sequence, delay):
    # reads keys from kvstore one by one using polling with a delay
    from kvstore import get_item
    from struct import unpack
    print(f'test_upload_counter: delay={delay:.2f}')

    my_initialize(watch_t_down)
    numbers = []

    for i in range(len(sequence)):
        keyname = f'dummy{i}'

        item = None

        while item is None:
            item = None
            try:
                item = get_item(keyname)
            except KeyError:
                pass

            if item is None:
                time.sleep(delay)

        numbers += [unpack('<H', item.value)[0]]

    print(f'numbers: {numbers}\nsum: {sum(numbers)}')
    return sum(numbers)

# copied from clear_s3.py
def clear():
    from kvstore.s3 import delete_file, list_files

    for filename in list_files():
        delete_file(filename)


def run_no_crash(sequence):
    clear()

    expected = sum(sequence)
    delay = 0.05
    test_upload(sequence, delay)
    result = test_upload_counter(sequence, delay)

    if expected != result:
        print('test no crash: failed')
    else:
        print('test no crash: ok')


"""
def run_uploader_crash(sequence):
    import threading
    clear()

    expected = sum(sequence)
    delay = 0.1

    wait_lock = threading.Lock()
    wait = True

    def upload_counter():
        test_upload_counter(sequence, delay)

        with wait_lock:
            wait = False

    t1 = threading.Thread(target=lambda: test_upload(sequence, delay))
    t2 = threading.Thread(target=upload_counter)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
"""

import threading
import ctypes

class thread_exc(threading.Thread):
    def __init__(self, name, func, **kwargs):
        threading.Thread.__init__(self, target=func, kwargs=kwargs)
        self.name = name

    def get_id(self):
        if hasattr(self, '_thread_id'):
            return self._thread_id

        for i, t in threading._active.items():
            if t is self:
                return i

    def raise_exc(self, exctype=None):
        tid = self.get_id()
        if exctype is None:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(SystemExit))
        else:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(exctype))
        if res != 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
            print(f'raise_exc: failed for tid={tid}')

# begin watcher customized code
from kvstore.config import config
from kvstore.database import store_database
from kvstore.item import new_multi_item

watcher_lock = threading.Lock()
watcher_running = False
watch_t_up = [None, None]
watch_t_down = [None, None]

def _watch_database():
    last_time_local = time.time()
    last_time_online = last_time_local
    running = True

    while running:
        with watcher_lock:
            if not watcher_running:
                running = False
                continue

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
    while config.CURRENT_MULTI_ITEM is None:
        time.sleep(0.1)

    running = True

    while running:
        with watcher_lock:
            if not watcher_running:
                running = False
                continue

        #print('check multi item', len(config.CURRENT_MULTI_ITEM))
        if len(config.CURRENT_MULTI_ITEM) >= config.MAX_MULTI_ITEM_SIZE:
            #print('multi item size', len(config.CURRENT_MULTI_ITEM))
            swap_multi_item()
        time.sleep(1)


def watcher():
    thread1 = threading.Thread(target=_watch_database, daemon=True)
    thread2 = threading.Thread(target=_watch_multi_item, daemon=True)

    with watcher_lock:
        watcher_running = True

    thread1.start()
    thread2.start()
    return thread1, thread2

def watcher_stop(t1, t2, wait=True):
    with watcher_lock:
        watcher_running = False

        if wait:
            t1.join()
            t2.join()


def my_initialize(lst):
    from kvstore.item import download_missing, load_data
    from kvstore.watcher import swap_multi_item
    from kvstore.database import database, load_database, database_set_loader

    #print(database())
    load_database()
    database_set_loader(load_data)
    print('db:', database())
    print('fetch db')
    download_missing()
    print('db:', database())
    print('watch daemons')
    lst[0], lst[1] = watcher()
    print('running')

# end watcher customized code

def run_uploader_crash(sequence):
    import time

    clear()
    expected = sum(sequence)
    delay = 0.05
    crash = 0.75

    wait_lock = threading.Lock()
    wait = True
    running = True

    while running:
        with wait_lock:
            if not wait:
                running = False
                continue

        t1 = thread_exc('uploader', lambda:test_upload(sequence, delay))
        print('start uploader')
        t1.start()
        print('started')

        t1.join(timeout=crash)
        print('timeout')
        if t1.is_alive():
            print('kill')
            t1.raise_exc(ValueError)
            t1.join()
            watcher_stop(watch_t_up[0], watch_t_up[1])
            print('restarting')
        else:
            print('no restart')
            running = False

    delay = 0.05
    #test_upload(sequence, delay)
    result = test_upload_counter(sequence, delay)

    if expected != result:
        print('test uploader crash: failed')
    else:
        print('test uploader crash: ok')

        

def main():
    import random

    # uploader will add 100 random int keys, downloader will load and sum them
    # uploader will eventually crash at some point, where it should pick up the old state
    # TODO let uploader crash
    count = 100
    sequence = [random.randint(0, 1024) for _ in range(count)]
    print(f'sequence: {sequence}\nsum: {sum(sequence)}')

    run_no_crash(sequence)
    #run_uploader_crash(sequence)
    # TODO more tests here


if __name__ == '__main__':
    main()
