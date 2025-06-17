#!/usr/bin/env bash

# [ -d /app ] || git clone https://github.com/erdincka/catchx.git /app

# cd /app

. $HOME/.local/bin/env
# uv init --app --name mesh --description "Data Mesh with Data Fabric" --author-from git
# uv add nicegui protobuf==3.20.* requests importlib_resources
# uv add faker 'pyiceberg[hive,pandas,s3fs]' sqlalchemy deltalake
# # apt install -y mapr-spark
# uv add geopy country_converter PyMySQL pycountry country_converter minio
unset VIRTUAL_ENV
# uv venv
# uv pip install 'protobuf==3.20.*'
# uv pip install ./maprdb_python_client-1.1.7-py3-none-any.whl
uv run main.py

# don't exit when service dies.
# sleep infinity
