from pathlib import Path

import yaml
from flask import Flask, Response, request
from flask_httpauth import HTTPTokenAuth

from postgres_connector import PostgresConnector
from s3_connector import S3Connector

app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

tokens = []


@auth.verify_token
def verify_token(token):
    return token in tokens


@app.route('/minio/events', methods=['POST'])
@auth.login_required
def index():
    request_json = request.json
    print(request_json['EventName'], request_json['Key'])

    db = PostgresConnector(config['postgres'])
    db.create_schema_if_not_exists()
    db.create_log_table_if_not_exists()
    column_data = db.get_columns(Path(request_json['Key']).parent)
    table_names_for_entities = db.create_entity_tables_if_not_exist(
        column_data, request_json['Key']
    )

    s3 = S3Connector(config['minio'])
    for df in s3.query_parquet(path=request_json['Key'], column_data=column_data):
        db.copy_into_table(table_names_for_entities, df)

    return Response(status=200)


if __name__ == '__main__':
    with open("entity_store/entity_store_config.yaml", 'r') as file:
        try:
            config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc
    tokens = config['webhook']['tokens']
    app.run(host='0.0.0.0', port=12346)
