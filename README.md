# feast-entity-store

This repository contains an extension to the feast feature store.

### ToDos
-  build container
-  check if deletion
-  handle delete operations


### Docker commands
- Build: `docker build --pull --rm -f "Dockerfile" -t feast-entity-store:latest --build-arg HTTP_PROXY=http://proxy:8080/ --build-arg HTTPS_PROXY=http://proxy:8080/ .`
- Run: `docker run --rm -it -v $PWD/config/entity_store_config.yaml:/app/entity_store/entity_store_config.yaml -p 12346:12346 feast-entity-store:latest`

### Extract from the MinIO docker-compose container
```
entity_store:
restart: on-failure
image: feast-entity-store:latest
container_name: feast-entity-store
volumes:
    - ./entity_store/entity_store_config.yaml:/app/entity_store/entity_store_config.yaml
expose:
    - 12346

minio:
restart: always
image: minio/minio:RELEASE.2021-03-01T04-20-55Z
container_name: feast_minio
environment:
    - MINIO_ROOT_USER=${AWS_ACCESS_KEY_ID:-access_key}
    - MINIO_ROOT_PASSWORD=${AWS_SECRET_ACCESS_KEY:-secret_key}
    - MINIO_NOTIFY_WEBHOOK_ENABLE=on
    - MINIO_NOTIFY_WEBHOOK_ENDPOINT=http://feast-entity-store:12346/minio/events
    - MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN=***
    - MINIO_NOTIFY_WEBHOOK_QUEUE_DIR=/queue
    - MINIO_API_SELECT_PARQUET=on
volumes:
    - ./minio/data:/data:Z
    - ./minio/queue:/queue:Z
ports:
    - "${MINIO_PORT:-19001}:9000"
command: server /data
```