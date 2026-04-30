FROM --platform=linux/amd64 erdincka/dfclient

EXPOSE 8000

COPY . /app/backend
WORKDIR /app/backend

RUN /root/.local/bin/uv venv
RUN /root/.local/bin/uv pip install -r requirements.txt
RUN /root/.local/bin/uv pip install 'protobuf==3.20.*'
RUN /root/.local/bin/uv pip install /app/backend/maprdb_python_client-1.1.7-py3-none-any.whl
RUN /root/.local/bin/uv pip install mapr-streams-python

ENV LD_LIBRARY_PATH=/opt/mapr/lib

CMD ["/root/.local/bin/uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
