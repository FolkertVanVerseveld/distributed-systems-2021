import typing

from kvstore.database import has_key, add_item, add_multi_item, update_multi_item, load_database, numerate_key, num_exists
from kvstore.exceptions import NotSupportedError
from kvstore.s3 import upload_file, download_file, delete_file, list_files
from kvstore.utils import random_string


class ItemBase:
    """The base for the item types.

    Args:
        key (str): The key of the item.
        is_uploaded (bool): Whether the item is uploaded. Default value is
            False.
        reloaded (bool): Whether the item is reloaded. Default value is False.
    """

    def __init__(self, key: str, is_uploaded: bool = False,
                 reloaded: bool = False):
        self._reloaded = reloaded
        self.key = key
        self._is_uploaded = is_uploaded

    @property
    def key(self) -> str:
        """Return the key of the item."""
        return self._key

    @key.setter
    def key(self, value: str):
        """Set the key for the current item.

        Args:
            value (str): The new value for the key.
        """
        if hasattr(self, '_key'):
            raise NotSupportedError('Please make a new instance.')
        if not self._reloaded and has_key(value):
            raise ValueError('Key already exists.')
        self._key = value

    @property
    def is_uploaded(self) -> bool:
        """Check if the item is uploaded."""
        return self._is_uploaded

    @is_uploaded.setter
    def is_uploaded(self, _):
        raise NotSupportedError('Uploaded status cannot be set manually.')

    def _reset_data(self):
        """Reset the data in the item."""
        self._data = None


class Item(ItemBase):
    """A single item.

    Args:
        key (str): The key of the item.
        value (bytes): The value of the item.
        is_uploaded (bool): If the item already uploaded. Default value is
            False.
        reloaded (bool): If the item is reloaded. Default value is False.
    """

    def __init__(self, key: str, value: bytes, is_uploaded: bool = False,
                 reloaded: bool = False):
        super().__init__(key, is_uploaded, reloaded)
        self._value = value

    def upload(self):
        """Write the data of the item and upload the item."""
        data = self._write_item()
        upload_file(create_filename(self._key), data)
        self._is_uploaded = True

    @property
    def value(self) -> bytes:
        """bytes: Get the value of the of item."""
        return self._value

    @value.setter
    def value(self, value: bytes):
        """Set the value of the item."""
        self._value = value

    @property
    def str(self) -> str:
        """str: The string representation of the value of the item."""
        return str(self._value, 'utf8')

    @property
    def bytes(self) -> bytes:
        """Return the bytes representation of the value of the item."""
        return self._value

    def __len__(self):
        """The length of the value of the item."""
        return len(self._value)

    def delete(self):
        """Delete the stored item itself if already stored."""
        if not self._is_uploaded:
            return None
        return delete_file(self._key)

    def _write_item(self) -> bytes:
        """Dump the item to a representation to be uploaded.

        This contain a list of bytes values concatentated with the NULL byte:
         - `s`
         - key
         - value

        Returns:
            bytes: The dumped data.
        """
        if hasattr(self, '_data') and self._data is not None:
            return self._data
        data = b's\0'
        data += bytes(self._key, 'utf8')
        data += b'\0'
        data += self._value
        self._data = data
        return self._data


class SubItem(Item):
    """The sub-item as part of the multi-item."""

    def upload(self):
        """Prevent uploading a single sub-item."""
        raise NotSupportedError('Please upload the multi item.')

    def regular_item(self, key: str):
        """Return the current sub-item as a regular item.

        Args:
            key (str): The key for the new regular item.

        Returns:
            Item: The new item with value copied from the current sub-item.
        """
        return Item(key, self._value)

    @Item.is_uploaded.setter
    def is_uploaded(self, value: bool):
        """Set whether the sub-item is uploaded or not."""
        self._is_uploaded = value

    def _write_item(self):
        """Prevent dumping a single sub-item."""
        raise NotSupportedError()


class MultiItem(ItemBase):
    """The multi-item holding sub-items.

    Args:
        key (str): The key of the multi-item. Default key is a randomized
            string.
        rawdata (bytes): The raw data to be read and used for the multi-item.
            This is optional.
        is_uploaded (bool): Whether the multi-item is already uploaded. Default
            value is False.
        reloaded (bool): Whether the multi-item is reloaded or not. Default
            value is False.
    """

    def __init__(self, key: str = None, rawdata: bytes = None,
                 is_uploaded: bool = False, reloaded: bool = False):
        super().__init__(key or self._create_key(), is_uploaded, reloaded)
        if rawdata is not None:
            self._items = self._read_item(rawdata)
        else:
            self._items = {}

    def upload(self):
        """Store the multi-item."""
        # dump the multi-item
        data = self._write_item()
        # upload the dumped data
        upload_file(create_filename(self._key), data)
        # and set is_uploaded to True for all
        self._is_uploaded = True
        for item in self._items.values():
            item.is_uploaded = True

    def has_item(self, key: str) -> bool:
        """Check if this multi-item has a sub-item with certain key.

        Args:
            key (str): The key to look for.

        Returns:
            bool: True if a sub-item exists with the given key.
        """
        return key in self._items

    def get_item(self, key: str) -> Item:
        """Get the sub-item with a certain key.

        Args:
            key (str): The key for the sub-item to get.

        Returns:
            SubItem: The sub-item with the key.
        """
        if not has_key(key):
            raise KeyError('Item not found.')
        return self._items[key]

    def delete_item(self, key: str):
        """Delete the a certain sub-item.

        Args:
            key (str): The key of the sub-item to remove.
        """
        if not has_key(key):
            raise KeyError('Item not found.')
        self._reset_data()
        del self._items[key]

    def append_item(self, item: Item):
        """Append an existing item to the multi-item.

        The actually added item is a sub-item with the key and value copied from
        the regular item.

        Args:
            item (Item): The item to add.
        """
        if has_key(item.key):
            raise KeyError('Key already exists.')
        self._reset_data()
        self._items[item.key] = SubItem(item.key, item.value)
        if len(self._items) == 1:
            add_multi_item(self)
        update_multi_item(self)

    def append_key_value(self, key: str, value: bytes):
        """Append a key-value pair to the multi-item.

        Args:
            key (str): The key of the item.
            value (bytes): The value of the item.
        """
        if has_key(key):
            raise KeyError('Key already exists.')
        return self.append_item(SubItem(key, value))

    @property
    def items(self) -> typing.List[Item]:
        """Get a list of sub-items.

        Returns:
            list of SubItem: The sub-items in this multi-item.
        """
        return list(self._items.values())

    @property
    def keys(self) -> typing.List[str]:
        """Get the list of keys of the sub-items.

        Returns:
            list of str: The keys of the sub-items in this multi-item.
        """
        return list(self._items.keys())

    def _read_item(self, rawdata: bytes) -> typing.Dict[str, Item]:
        """Load the sub-items from the provided raw data.

        Args:
            rawdata (bytes): The raw data to loaded, formatted according to the
                docstring on function `_write_item` of class `MultiItem`.

        Returns:
            list of SubItem: The list of loaded sub-items.
        """
        log = []
        items = {}
        # get the number of items.
        num_items, rawdata = rawdata.split(b'\0', 1)
        # for the each sub-item, get the key and length of the value
        for _ in range(int(num_items)):
            key, rawdata = rawdata.split(b'\0', 1)
            length, rawdata = rawdata.split(b'\0', 1)
            key = str(key, 'utf8')
            length = int(length)
            log.append((key, length))
        # using the length of the sub-item, load the values from remaining raw
        # data.
        for key, length in log:
            value, rawdata = rawdata[:length], rawdata[length:]
            if len(value) != length:
                raise ValueError('Could not correctly read {} from {}.'
                                 .format(key, self._key))
            items[key] = SubItem(key, value, is_uploaded=self._is_uploaded,
                                 reloaded=self._reloaded)
        # check if no raw data is left over.
        if len(rawdata) > 0:
            raise ValueError('Multi item has left over raw data.')
        return items

    def _write_item(self) -> bytes:
        """Dump the multi-item to a correct representation for uploading.

        The data is formatted as follows. The first set of variables in bytes
        are concatenated by a NULL byte. These are
         - `m`
         - key of multi-item
         - number of sub-items
        for each sub-item, the following are added, again concatenated with a
        NULL byte
         - key of the sub-item
         - length of the value of the sub-item
        and finally, in the same order as previously, the actual values of the
        sub-items are added to the data. This time not concatenated with a NULL
        byte, since the value itself could contain a NULL byte, and thus NULL
        cannot be used as a delimiter.

        Returns:
            bytes: The dumped data of the multi-item.
        """
        if hasattr(self, '_data') and self._data is not None:
            return self._data
        # add general data about the multi-item
        data = b'm\0'
        data += bytes(self._key, 'utf8')
        data += b'\0'
        data += bytes(str(len(self._items)), 'utf8')
        data += b'\0'
        # list all sub-items from dictionary to ensure equal order upon reuse
        items = list(self._items.items())
        # for each sub-item, add the key and length of the value
        for key, item in items:
            data += bytes(item.key, 'utf8')
            data += b'\0'
            data += bytes(str(len(item)), 'utf8')
            data += b'\0'
        # for each sub-item, add the value itself.
        for _, item in items:
            data += item.value
        self._data = data
        return self._data

    def __len__(self) -> int:
        """int: The number of items in the multi-item."""
        return len(self._items)


def load_data(data: bytes, is_uploaded: bool = True,
              reloaded: bool = False) -> typing.Union[Item, MultiItem]:
    """Load raw data as a regular item or multi-item.

    If the data starts with a `s`, this is a regular item, and if it starts with
    an `m`, it is a multi-item.

    Args:
        data (bytes): The raw data to be loaded.
        is_uploaded (bool): Whether the item to be loaded is already uploaded.
            Default value is True.
        reloaded (bool): Whether the item to be loaded is being reloaded.
            Default value is False.

    Returns:
        Item or MultiItem: The loaded item.
    """
    type_, key, rawdata = data.split(b'\0', 2)
    key = str(key, 'utf8')
    if type_ == b's':
        return Item(key, rawdata, is_uploaded, reloaded=reloaded)
    elif type_ == b'm':
        return MultiItem(key, rawdata, is_uploaded, reloaded=reloaded)
    raise ValueError('Unknown item type.')


def new_multi_item() -> MultiItem:
    """Create a new multi-item.

    Note:
        The new multi-item has a key without much meaning, so it is simply a
        random string of length 10. The only keys with meaning are the sub-items
        in the multi-item.

    Returns:
        MultiItem: The new created multi-item.
    """
    while True:
        key = random_string(10)
        # make sure the random string is not in use yet as key
        if not has_key(key):
            break
    return MultiItem(key)


def new_item(key: str, value: bytes) -> Item:
    """Create a new regular item.

    Args:
        key (str): The key of the item.
        value (bytes): The value of the item.

    Returns:
        Item: The new regular item.
    """
    return Item(key, value)


def create_filename(key: str):
    """Create a filename for a certain registered key.

    A registered key in the database either has a number assigned for storing,
    or will be assigned a new number of be stored under.

    Args:
        key (str): The key to create the filename for.

    Returns:
        str: The filename for the key.
    """
    num = numerate_key(key)
    filename = str(num) + '_' + key
    return filename


def load_filename(key: str):
    """Load an existing filename for the certain key.

    Differently from function `create_filename`, this function will not create a
    new filename if no filename exists yet, it will only return an existing
    filename if found.

    Args:
        key (str): The key to load the filename for.

    Returns:
        str: The filename for the key.
    """
    if not has_num(key):
        raise ValueError('Key has no number.')
    return create_filename(key)


def download_missing():
    """Download the stored items that are not in the database.

    These items are found using their numbers. If the numbers are not yet in the
    database, the data for the items is loaded, and the items themselves are
    added to both the reference table and the index database.
    """
    for filename in list_files():
        print(f'download_missing: filename={filename}')
        if '_' not in filename:
            continue
        num = filename.split('_', 1)[0]
        if not num.isnumeric():
            continue
        num = int(num)
        if num_exists(num):
            continue
        print(f'download_missing: num={num}')
        rawdata = download_file(filename)
        item = load_data(rawdata, is_uploaded=True)
        add_multi_item(item)
        numerate_key(item.key, num)

