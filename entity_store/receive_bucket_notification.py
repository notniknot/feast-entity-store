from datetime import datetime
from pathlib import Path

import yaml
from flask import Flask, Response, request
from flask_httpauth import HTTPTokenAuth

from connectors.postgres_connector import PostgresConnector
from connectors.s3_connector import S3Connector

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
    started = datetime.now()
    try:
        db = PostgresConnector(config['postgres'])
    except Exception:
        return Response(status=200)

    status = 'success'
    status_msg = None

    try:
        db.create_schema_if_not_exists()
        db.create_log_table_if_not_exists()
        column_data = db.get_columns(Path(request_json['Key']).parent)
        table_names_for_entities = db.create_entity_tables_if_not_exist(
            column_data, request_json['Key']
        )
        db.create_view_if_not_exists(table_names_for_entities, column_data)
        s3 = S3Connector(config['minio'])
        for df in s3.query_parquet(path=request_json['Key'], column_data=column_data):
            db.copy_into_table(table_names_for_entities, df)
    except Exception as ex:
        status = 'failed'
        status_msg = str(ex)

    ended = datetime.now()
    try:
        db.add_log(
            {
                'started': started,
                'ended': ended,
                'status': status,
                'status_msg': status_msg,
                'entity_names': column_data.get('entity_names') if column_data else None,
                'feature_table': column_data.get('feature_table') if column_data else None,
                'path': request_json['Key'],
            }
        )
    except Exception as ex:
        print('Could not save log:', ex)

    return Response(status=200)


if __name__ == '__main__':
    with open("entity_store/entity_store_config.yaml", 'r') as file:
        try:
            config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc
    tokens = config['webhook']['tokens']
    app.run(host=config['flask']['host'], port=config['flask']['port'])
