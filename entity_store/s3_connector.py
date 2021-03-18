from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd
from pandas.core.common import flatten


class S3Connector:

    instance = None

    @classmethod
    def get_instance(cls) -> S3Connector:
        if cls.instance is None:
            cls.instance = cls()
        return cls.instance

    def __init__(self, config):
        self.logger = logging.getLogger('s3_connector')
        try:
            self.client = boto3.client('s3', **config)
        except Exception:
            self.logger.exception('Exception occurred while connecting to the S3 storage')

    def _split_parquet_path(self, path):
        parts = Path(path).parts
        return parts[0], Path(*parts[1:])

    def _merge_cols(self, column_data):
        columns = [
            column_data['entity_names'],
            column_data['created_timestamp_column'],
            column_data['timestamp_column'],
        ]
        columns_list = list(flatten(columns))
        column_string = ', '.join(columns_list)
        return columns_list, column_string

    def query_parquet(self, path, column_data):
        bucket, key = self._split_parquet_path(path)
        column_list, column_string = self._merge_cols(column_data)
        try:
            response = self.client.select_object_content(
                Bucket=bucket,
                Key=str(key),
                ExpressionType='SQL',
                Expression=f"SELECT {column_string} FROM S3Object",
                InputSerialization={'Parquet': {}},
                OutputSerialization={'CSV': {}},
            )
        except Exception as ex:
            self.logger.exception('Exception occurred while retrieving parquet')
            raise ex

        try:
            for event in response['Payload']:
                if 'Records' in event:
                    df = pd.read_csv(
                        StringIO(event['Records']['Payload'].decode('utf-8')),
                        sep=',',
                        engine='c',
                        lineterminator='\n',
                        names=column_list,
                    )
                    df['feature_table'] = column_data['feature_table']
                    df['path'] = path
                    df[column_data['timestamp_column']] = pd.to_datetime(
                        df[column_data['timestamp_column']], unit='us'
                    )
                    df[column_data['created_timestamp_column']] = pd.to_datetime(
                        df[column_data['created_timestamp_column']], unit='us'
                    )
                    yield df
        except Exception as ex:
            self.logger.exception('Exception occurred while iterating response payload')
            raise ex
