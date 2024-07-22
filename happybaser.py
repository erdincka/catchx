import happybase
import logging

from nicegui import app

from common import *

logger = logging.getLogger("happybaser")

def connect():

    try:
        connection = happybase.Connection(app.storage.general.get('cluster', 'localhost'), table_prefix=DATA_PRODUCT)
        # connection.open() # not needed since autoconnect is True by default

        print(connection.tables())

    except Exception as error:
        logger.warning(error)


    connection.create_table(
        'mytable',
        {'cf1': dict(max_versions=10),
         'cf2': dict(max_versions=1, block_cache_enabled=False),
         'cf3': dict(),  # use defaults
        }
    )

# table = connection.table('mytable')

# row = table.row(b'row-key')
# print(row[b'cf1:col1']) 

# for key, data in table.scan():
#     print(key, data)

# table.put(b'row-key', {b'cf:col1': b'value1',
#                        b'cf:col2': b'value2'})

