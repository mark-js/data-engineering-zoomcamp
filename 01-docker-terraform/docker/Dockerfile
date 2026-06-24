FROM python:3.12.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_data.py .
COPY data/ ./data/

ENTRYPOINT [ "python", "ingest_data.py" ]