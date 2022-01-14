class NotSupportedError(NotImplementedError):
    """The method is not supported. Variant of the NotImplementedError."""
    pass


class S3FileNotFoundError(KeyError):
    """The file on AWS S3 is not found."""
    pass

