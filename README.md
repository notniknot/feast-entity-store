# feast-entity-store

This repository contains an extension to the feast feature store.

### Extract from the MinIO docker-compose container
```
minio:
    restart: always
    image: minio/minio:RELEASE.2021-03-01T04-20-55Z
    container_name: mlflow_minio
    environment:
        - MINIO_ROOT_USER=${AWS_ACCESS_KEY_ID:-access_key}
        - MINIO_ROOT_PASSWORD=${AWS_SECRET_ACCESS_KEY:-secret_key}
        - MINIO_NOTIFY_WEBHOOK_ENABLE=on
        - MINIO_NOTIFY_WEBHOOK_ENDPOINT=http://***:12346/minio/events
        - MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN=***
        - MINIO_NOTIFY_WEBHOOK_QUEUE_DIR=/queue
    volumes:
        - ./minio/data:/data:Z
        - ./minio/queue:/queue:Z
    ports:
        - "${MINIO_PORT:-19000}:9000"
    networks:
        - backend
    command: server /data
```