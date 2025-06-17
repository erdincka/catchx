FROM --platform=linux/amd64 erdincka/dfclient

EXPOSE 3000

COPY . /app

WORKDIR /app

RUN /root/.local/bin/uv venv
RUN /root/.local/bin/uv pip install -r requirements.txt
RUN /root/.local/bin/uv pip install 'protobuf==3.20.*'
RUN /root/.local/bin/uv pip install ./maprdb_python_client-1.1.7-py3-none-any.whl
RUN /root/.local/bin/uv pip install mapr-streams-python

ENTRYPOINT [ "/app/start.sh" ]
