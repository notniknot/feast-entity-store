FROM python:3.8.8-slim-buster

COPY app.py .

ENV PYTHONUNBUFFERD="1"
RUN pip install --no-cache-dir minio
RUN python app.py