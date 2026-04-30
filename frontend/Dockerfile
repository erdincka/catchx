FROM python:3.12-slim

EXPOSE 3000

COPY . /app/frontend
WORKDIR /app/frontend

RUN pip install --no-cache-dir uv
RUN uv venv
RUN uv pip install -r requirements.txt

CMD ["uv", "run", "python", "main.py"]
