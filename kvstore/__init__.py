from kvstore.config import config
from kvstore.database import get_item, load_database, store_database, database, \
    database_set_loader
from kvstore.item import new_multi_item, download_missing, load_data, SubItem
from kvstore.watcher import watcher, swap_multi_item

__all__ = ('new_item', 'get_value')


def initialize(fetch=True, dbg=False):
    """Initialize the KV store.

    This function should be called before any others.
    """
    def dump():
        if dbg: print('db:', database())

    dump()
    load_database()

    database_set_loader(load_data)
    dump()

    if fetch: download_missing()
    dump()

    watcher()


def new_item(key: str, value: bytes) -> SubItem:
    """Create a new key-value item in the current multi-item.

    Args:
        key (str): The key.
        value (bytes): The value.

    Returns:
        SubItem: The created sub-item.
    """
    multi_item = config.CURRENT_MULTI_ITEM
    if multi_item is None:
        config.CURRENT_MULTI_ITEM = new_multi_item()
        return new_item(key, value)
    multi_item.append_key_value(key, value)
    return get_item(key)


def get_value(key: str) -> SubItem:
    """Get the sub-item for a certain key.

    Args:
        key (str): The key to get the sub-item for.

    Returns:
        SubItem: The retrieved sub-item.
    """
    return get_item(key)


def store_multi_item():
    """Create a new multi item and swap with the old one."""
    swap_multi_item()

