import happybase

from nicegui import app

connection = happybase.Connection(app.storage.general.get('cluster', 'localhost'), table_prefix='myproject')
# connection.open() # not needed since autoconnect is True by default

print(connection)

print(connection.tables())

# connection.create_table(
#     'mytable',
#     {'cf1': dict(max_versions=10),
#      'cf2': dict(max_versions=1, block_cache_enabled=False),
#      'cf3': dict(),  # use defaults
#     }
# )

# table = connection.table('mytable')

# row = table.row(b'row-key')
# print(row[b'cf1:col1']) 

# for key, data in table.scan():
#     print(key, data)

# table.put(b'row-key', {b'cf:col1': b'value1',
#                        b'cf:col2': b'value2'})

