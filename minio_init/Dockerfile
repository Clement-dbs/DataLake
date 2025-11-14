FROM python:3.11-alpine

WORKDIR /app

# installer le client Python pour MinIO
RUN pip install minio

COPY buckets.py /app/buckets.py

CMD ["python", "-u", "buckets.py"]
