FROM erdincka/dfclient

EXPOSE 8000

COPY . /app
WORKDIR /app

RUN /root/.local/bin/uv venv .venv
RUN /root/.local/bin/uv pip install -r /app/requirements.txt
RUN /root/.local/bin/uv pip install 'protobuf==3.20.*'
RUN /root/.local/bin/uv pip install /app/maprdb_python_client-1.1.7-py3-none-any.whl
RUN /root/.local/bin/uv pip install mapr-streams-python

ENV LD_LIBRARY_PATH=/opt/mapr/lib

CMD ["/app/.venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
