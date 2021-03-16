# feast-entity-store

This repository contains an extension to the feast feature store.

### ToDos
- Write entity-ids to table
- For every entity an own table
- Create lookup in which file entity-id is stored
  - m:n dependency

- table: entityX_ids
  - unique_key
  - entity_id
  - event_timestamp
  - created_timestamp
- table: entityX_ids_files
  - entity_id
  - file_id (filename?)
- table: entityX_files
  - file_id (filename?)
  - file_path
- view: entityX_ids but only last value

- on_put
  - get column names
    - query data_sources
    - get created_timestamp name, event_timestamp name
    - join table with feature_tables
    - join feature_tables with feature_tables_entities_v2
    - join feature_tables_entities_v2 with entities_v2
    - get entities
  - tables + view already present? Create if not
  - query parquet with retrieved table names
  - insert into entityX_ids
  - insert into entityX_files
  - insert into entityX_ids_files

- get methods necessary?
  - get engine
  - get all entities between timestamps
  - get all entities 

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