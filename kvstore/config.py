class Config:
    """The configuration of the key-value store."""
    CURRENT_MULTI_ITEM = None
    MAX_MULTI_ITEM_SIZE = 3
    DATABASE_DUMP_INTERVAL_ONLINE = 2 # sec
    DATABASE_FILENAME = 'count_test'

"""Lower case variable for access."""
config = Config

