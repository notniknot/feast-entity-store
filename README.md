<h1 align="center">
	<img
		width="400"
		alt="feast-entity-store icon"
		src="docs/fes_icon.png"
    >
</h1>

<h3 align="center">
	An extension to <a href="https://feast.dev/" target="_blank">Feast (Feature Store)</a> to support feature retrieval from MinIO without knowing the exact Entity-IDs
</h3>

<p align="center">
    <img src="https://img.shields.io/badge/language-python-green">
    <img src="https://img.shields.io/badge/codestyle-black-black">
    <img src="https://img.shields.io/badge/last commit-yesterday-inactive">
    <img src="https://img.shields.io/badge/commit acitvity-none-inactive">
</p>

<p align="center">
  <a href="#addressed-issues">Addressed Issues</a> •
  <a href="#target-group">Target Group</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#setup">Setup</a> •
  <a href="#todos">ToDos</a>
</p>

# Addressed Issues
This repro addresses the limitation of Feast regarding the necessity to specify the entity identifiers when retrieving features either from the offline store or the online store. In many use cases I encountered, I did not know the identifiers. This concerns the following scenarios:
-  Batch prediction
-  Model training

Additionally, I wanted to define a timespan for the event timetamp of an entity identifier (e.g. "give me all features for entity identifiers with an event timestamp between 2020-01-01 and 2020-12-31").

-> See similar [issue](https://github.com/feast-dev/feast/issues/1361)

# Target Group
This repo is for engineers who encountered the same problems as described and use MinIO in their Feast-Setup as their offline feature store.

# Architecture

![architecture](./docs/fes_architecture.png)

# Setup
## Integration in the Feast-Docker-Compose-Setup
1. Fill entity_store_config.yaml
2. Configure minio with set_bucket_notification
3. Provide files
4. Create service for python application

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

## Setup with a standalone Docker-Container
1. Fill entity_store_config.yaml
2. Configure minio with set_bucket_notification
3. Build image
4. Run container

- Build: `docker build --pull --rm -f "Dockerfile" -t feast-entity-store:latest --build-arg HTTP_PROXY=http://proxy:8080/ --build-arg HTTPS_PROXY=http://proxy:8080/ .`
- Run: `docker run --rm -it -v $PWD/config/entity_store_config.yaml:/app/entity_store/entity_store_config.yaml -p 12346:12346 feast-entity-store:latest`


## Result
|                                                          1                                                           |                                                          2                                                           |                                                          3                                                           |                                                          4                                                           |
| :------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------: |
| <img src="https://raw.githubusercontent.com/notniknot/feast-entity-store/....PNG" title="Screenshot 1" width="100%"> | <img src="https://raw.githubusercontent.com/notniknot/feast-entity-store/....PNG" title="Screenshot 2" width="100%"> | <img src="https://raw.githubusercontent.com/notniknot/feast-entity-store/....PNG" title="Screenshot 3" width="100%"> | <img src="https://raw.githubusercontent.com/notniknot/feast-entity-store/....PNG" title="Screenshot 4" width="100%"> |


# ToDos
-  build container
-  check if deletion
-  handle delete operations