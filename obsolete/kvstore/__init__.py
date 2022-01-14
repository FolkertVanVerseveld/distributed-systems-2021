import typing

from kvstore.exceptions import NotSupportedError
from kvstore.s3 import upload_file, download_file, delete_file


# TODO fix all types

class KVItem:
    def __init__(self, key: str, value: typing.Optional[bytes] = None):
        self._key = key
        self._value = value
        self.is_multi = False
        self.hash = ''

    def store(self):
        if value is None:
            raise ValueError('No value assigned yet.')
        return put_object(self._key, self._value, save=True)

    def __len__(self) -> int:
        return len(self._value)


class MultiKVItem(KVItem):
    def __init__(self, key: str):
        super().__init__(key, b'')
        self._log = {}
        self.is_multi = True

    @KVItem.value.setter
    def value(self, *args, **kwargs):
        raise NotSupportedError()

    def add_value(self, key: str, value: bytes):
        # we get the offset before writing the new value so we do not store the
        # offset and length in the log before successfully writing.
        offset = len(self._value)
        self._value += value
        self._log[key] = (offset, len(value))

    def remove_value(self, key: str):
        if key not in self._log:
            raise ValueError('This key was not stored yet.')
        offset, length = self._log[key]
        self._value = self._value[:offset] + self._value[offset+length:]
        del self._log[key]
        for key, (o, l) in self._log.items():
            if o > offset:
                self._log[key] = (o-length, l)

    def get_value(self, key: str):
        if key not in self._log:
            raise ValueError('This key was not stored yet.')
        offset, length = self._log[key]
        return self._value[offset:offset+length]

    def __iter__(self):
        for k, (offset, length) in self._log.items():
            yield k, offset, length

    def __contains__(self, value: typing.Union[str, bytes]) -> bool:
        if type(value) is str:
            return value in self._log
        elif type(value) is bytes:
            for offset, length in self._log.values():
                if length == len(value) \
                    and self._value[offset:offset+length] == value:
                    return True
            return False
        raise TypeError('Can only check for key or data existence.')

