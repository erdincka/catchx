FROM python:3.12-slim

EXPOSE 3000

COPY . /app/frontend
WORKDIR /app/frontend

# Install venv outside /app/frontend so volume mounts don't shadow it
ENV VIRTUAL_ENV=/opt/venv/frontend
RUN pip install --no-cache-dir uv
RUN uv venv $VIRTUAL_ENV
RUN uv pip install -r requirements.txt

ENV PATH="/opt/venv/frontend/bin:$PATH"

CMD ["python", "main.py"]
