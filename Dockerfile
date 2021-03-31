FROM python:3.8.8-slim-buster

WORKDIR /app

ENV PYTHONUNBUFFERD="1"

RUN pip install --no-cache-dir psycopg2-binary Flask Flask-HTTPAuth pandas boto3 pyyaml

COPY src entity_store

CMD [ "python", "entity_store/receive_bucket_notification.py" ]