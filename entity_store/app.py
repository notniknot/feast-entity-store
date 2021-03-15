#     - MINIO_NOTIFY_WEBHOOK_ENABLE=on
#     - MINIO_NOTIFY_WEBHOOK_ENDPOINT=http://192.168.178.21:1234/minio/events
#     - MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN=***
#     - MINIO_NOTIFY_WEBHOOK_QUEUE_DIR=/queue
# volumes:
#     - ./minio/data:/data
#     - ./minio/queue:/queue


from minio import Minio
from minio.notificationconfig import NotificationConfig, PrefixFilterRule, QueueConfig


# Create client with access and secret key.
client = Minio("192.168.178.21:19000", "access_key", "secret_key")

config = NotificationConfig(
    queue_config_list=[
        QueueConfig(
            "QUEUE-ARN-OF-THIS-BUCKET",
            ["s3:ObjectCreated:*"],
            config_id="1",
            prefix_filter_rule=PrefixFilterRule("abc"),
        ),
    ],
)
client.set_bucket_notification("my-bucket", config)
