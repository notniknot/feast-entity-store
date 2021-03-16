import yaml
from minio import Minio
from minio.notificationconfig import NotificationConfig, QueueConfig, SuffixFilterRule

if __name__ == '__main__':
    with open("entity_store/entity_store_config.yaml", 'r') as file:
        try:
            config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc

    client = Minio(
        endpoint=config['minio']['endpoint'],
        access_key=config['minio']['access_key'],
        secret_key=config['minio']['secret_key'],
        secure=False,
    )

    config = NotificationConfig(
        queue_config_list=[
            QueueConfig(
                "arn:minio:sqs::_:webhook",
                ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                config_id="1",
                suffix_filter_rule=SuffixFilterRule(".parquet"),
            ),
        ],
    )
    client.set_bucket_notification("feast", config)
    print('Successfuly set notification')
