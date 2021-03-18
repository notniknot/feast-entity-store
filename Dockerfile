FROM python:3.8.8-slim-buster

COPY entity_store .

ENV PYTHONUNBUFFERD="1"
RUN pip install --no-cache-dir psycopg2-binary Flask Flask-HTTPAuth pandas boto3
RUN entity_store/python receive_bucket_notification.py