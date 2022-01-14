# see the old/ directory for the sqlite3 code, I though we might just go with an in-memory database.
import copy
import enum
import json
import pickle
import typing

from kvstore.exceptions import S3FileNotFoundError
from kvstore.s3 import download_file, upload_file, delete_file, list_files

DATABASE_TYPE = dict
ITEM_TYPES = typing.Union['Item', 'MultiItem']
S3_DATABASE_FILENAME = 'kvstore_database.pkl'


class Database:
    """Stores the references to the database and the items."""
    DATABASE = None
    REFERENCES = {}
    LOADER = None


class ItemType(enum.Enum):
    """The possible types of the items."""
    MULTI = 1
    SUB = 2
    REGULAR = 3


def database() -> dict:
    """Get the current database."""
    return Database.DATABASE


def references() -> dict:
    """Get the current references."""
    return Database.REFERENCES


def with_context(type_: str):
    """Run a function with either the database or the item references as arg.

    Args:
        type_ (str): Either `database` or `references` for which to load.
    """
    def with_database(function):
        def wrapper(*args, **kwargs):
            if type_ == 'database':
                data = database()
            elif type_ == 'references':
                data = references()
            return function(data, *args, **kwargs)
        return wrapper
    return with_database


def database_set_loader(loader):
    Database.LOADER = loader


def dump_database() -> bytes:
    """Dump the database and return.

    The database is copied and any items that have not yet been stored are
    removed. The remaining data is then dumped with pickle and returned.

    Returns:
        bytes: The pickled data.
    """
    if database() is None:
        return None
    # remove items in the to-be-uploaded database that are not uploaded yet.
    data = copy.deepcopy(database())
    removed = set()
    # first remove multi items, and keep a set of removed multi items.
    for name, d in database()['items'].items():
        if d['type'] == ItemType.MULTI and 'num' not in d:
            del data['items'][name]
            removed.add(name)
    # then remove all sub items part of these multi items.
    for name, d in database()['items'].items():
        if d['type'] == ItemType.SUB and d['part-of'] in removed:
            del data['items'][name]
    return pickle.dumps(data)


def load_database():
    """Load the latest stored database.

    The database is loaded from S3 and loaded with pickled, after which it is
    stored. If no database was previously stored, an empty database and empty
    reference table is created.
    """
    try:
        data = download_file(S3_DATABASE_FILENAME)
    except S3FileNotFoundError:
        Database.DATABASE = {
            'items': {},
            'nums': {},
            'current_num': 0
        }
        return None
    Database.DATABASE = pickle.loads(data)


def store_database(local: typing.Optional[str] = S3_DATABASE_FILENAME):
    """Store the database.

    The database is dumped and stored online.

    Args:
        local (str or None): Also store the database locally at this filename.
    """
    print('storing database')
    data = dump_database()
    if local:
        with open(local, 'wb') as f:
            f.write(data)
    return upload_file(S3_DATABASE_FILENAME, data)


def _delete_database(from_s3: bool = False):
    """Delete the current database.

    Args:
        from_s3 (bool): If True, remove the current stored database.
    """
    Database.DATABASE = None
    if from_s3:
        return delete_file(S3_DATABASE_FILENAME)


@with_context('references')
def reference_item(references, item):
    """Reference an item in the reference table.

    If the item to reference is a multi-item, the sub-items are referenced
    individually as well.

    Args:
        item (Item): The item to reference.
    """
    if hasattr(item, 'items'):
        for sub in item.items:
            reference_item(sub)
        return None
    references[item.key] = item


@with_context('references')
def dereference_item(references, item):
    """Dereference an item.

    Remove all references for the item in the reference table, but leaves the
    item in the database. If the item is a multi-item, all sub-items are removed
    from the reference table as well.

    Args:
        item (Item): The item to dereference.
    """
    if hasattr(item, 'items'):
        for sub in item.items:
            dereference_item(sub)
        return None
    if item.key in references:
        del references[item.key]


@with_context('references')
@with_context('database')
def get_item(database, references, key: str) -> 'Item':
    """Get an item from the reference table.

    The item should be in the database, but may not be in the reference table
    yet. If the item is not in the reference table, it is downloaded and loaded
    and added to the reference table.

    Args:
        key (str): The key for the item to get.

    Returns:
        SubItem: The sub-item to get.
    """
    if not has_key(key):
        raise KeyError('Item with key does not exist.')
    key_data = database['items'][key]
    if key not in references:
        if key_data['type'] == ItemType.SUB:
            multi_item_name = key_data['part-of']
            if multi_item_name not in database['items']:
                raise KeyError('Multi item does not exist. Faulty database?')
            multi_item = database['items'][multi_item_name]
            if 'num' not in multi_item:
                raise Exception('Multi item was never uploaded.')
            filename = str(multi_item['num']) + '_' + multi_item_name
            rawdata = download_file(filename)
        elif key_data['type'] == ItemType.REGULAR:
            rawdata = download_file(key)
        item = Database.LOADER(rawdata, reloaded=True)
        reference_item(item)
    if key in references:
        return references[key]
    else:
        raise Exception('Could not load item.')


@with_context('database')
def has_key(database: DATABASE_TYPE, key: str):
    """Check if a key exists in the database.

    Args:
        key (str): The key to check.

    Returns:
        bool: True if the key exists, else False.
    """
    return key in database['items']


@with_context('database')
def has_num(database: DATABASE_TYPE, key: str):
    """Check if a key has a number assigned (if it is stored or not).

    Args:
        key (str): The key to check.

    Returns:
        bool: True if the key has a number assigned, else False.
    """
    if not has_key(key):
        raise KeyError('Key is not in database.')
    return 'num' in database['items'][key]


@with_context('database')
def num_exists(database: DATABASE_TYPE, num: int):
    """Check if a number for an item exists.

    Args:
        num (int): The number to check for.

    Returns:
        bool: True if the number exists, else False.
    """
    return num in database['nums']


@with_context('database')
def _register_subs(database: DATABASE_TYPE, item: 'MultiItem'):
    """Register the sub-item in a multi-item.

    Note:
        This is a private function and should not be used.

    Args:
        item (MultiItem): The multi-item to register the sub-items from.
    """
    assert has_key(item.key)
    for sub in item.items:
        if sub.key in database['items']:
            continue
        database['items'][sub.key] = {
            'type': ItemType.SUB,
            'part-of': item.key
        }


@with_context('database')
def add_item(database: DATABASE_TYPE, item: 'Item'):
    """Add a simple item to the database.

    Args:
        item (Item): The item to add.
    """
    if item.key in database['items']:
        raise KeyError('Key already in database.')
    database['items'][item.key] = {
        'type': ItemType.REGULAR
    }
    reference_item(item)


@with_context('database')
def add_multi_item(database: DATABASE_TYPE, item: 'MultiItem'):
    """Add a multi item to the database and references table.

    Args:
        item (MultiItem): The multi item to add.
    """
    if item.key in database['items']:
        raise KeyError('Key already in database.')
    database['items'][item.key] = {
        'type': ItemType.MULTI
    }
    _register_subs(item)
    reference_item(item)


@with_context('database')
def update_multi_item(database: DATABASE_TYPE, item: 'MultiItem'):
    """Update a multi-item.

    The sub-items are updated, after which the multi-item itself is updated.

    Args:
        item (MultiItem): The multi-item to be updated.
    """
    if not has_key(item.key):
        raise KeyError('Multi item not found.')
    _register_subs(item)
    reference_item(item)


@with_context('database')
def delete_item(database: DATABASE_TYPE, item: ITEM_TYPES, allow_sub: bool = False):
    """Delete an item.

    If the item to be deleted is a MultiItem, the SubItems are individually
    deleted first, after which the entire MultiItem is removed.

    Note:
        'Remove' in this case means 'dereference'. The item will not be in the
        database index or in the references table anymore.

    Args:
        item (Item or SubItem or MultiItem): The item to be removed.
        allow_sub (bool): Allow a sub-item to be removed. Default is False.
    """
    if item.key not in database['items']:
        raise KeyError('Key is not registered in database.')
    item_data = database['items'][item.key]
    type_ = item_data['type']
    if type_ == ItemType.SUB and not allow_sub:
        raise ValueError('Cannot delete a sub-item.')
    if type_ == ItemType.MULTI:
        for sub in item.items:
            delete_item(sub, allow_sub=True)
    if type_ != ItemType.SUB:
        n = database['items'][item.key]['num']
        if n:
            del database['nums'][n]
    del database['items'][item.key]
    dereference_item(item)


@with_context('database')
def numerate_key(database: DATABASE_TYPE, key: str, num: int = None):
    """Numerate a key.

    The number is used to track which items have been stored and which have not
    yet been stored. If a number already exists for the item, it is simply
    returned. If no number exists yet, a new number is created by taking the
    next lowest number after the largest existing number, or a given number
    `num` is used.

    Args:
        key: The key to numerate or find a number for.
        num: The number to use if not number found. Default value is None.

    Returns:
        int: The number.
    """
    if key not in database['items']:
        raise KeyError('Key is not registered in database.')
    item_data = database['items'][key]
    if item_data['type'] == ItemType.SUB:
        raise ValueError('Sub-item cannot be numerated.')
    if 'num' in item_data:
        return item_data['num']
    if num is None:
        num = database['current_num'] + 1
        new_high = num
    else:
        new_high = num if num > database['current_num'] else database['current_num']
    item_data['num'] = num
    database['nums'][num] = key
    database['current_num'] = new_high
    return num


def numerate_item(item: ITEM_TYPES):
    """Numerate an item.

    Args:
        item (Item or MultiItem): The item to create or get a number for.

    Returns:
        int: The number.
    """
    return numerate_key(item.key)

