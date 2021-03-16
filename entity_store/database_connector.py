from __future__ import annotations

import logging
from datetime import datetime
from contextlib import contextmanager
from os import closerange
from typing import Literal

from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import SimpleConnectionPool


class MissingDataError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)


def exception_decorator(wrapped_function):
    def _wrapper(*args, **kwargs):
        try:
            result = wrapped_function(*args, **kwargs)
        except Exception as error:
            logging.getLogger('database_connector').exception(
                'Exception occurred in %s.', wrapped_function.__name__
            )
            raise type(error)(f'Exception occurred in {wrapped_function.__name__}: {str(error)}')
        return result

    return _wrapper


class DatabaseConnector:

    db_instance = None

    @classmethod
    def get_db_instance(cls) -> DatabaseConnector:
        if cls.db_instance is None:
            cls.db_instance = cls()
        return cls.db_instance

    def __init__(self, config):
        self.logger = logging.getLogger('database_connector')
        try:
            self.pool = SimpleConnectionPool(1, 10, **config)
            self.schema = 'entity_lookup'
        except Exception:
            logging.getLogger('database_connector').exception(
                'Exception occurred while connecting to the database'
            )

    def _get_dict_cursor(self, conn):
        return conn.cursor(cursor_factory=RealDictCursor)

    def _get_connection(self):
        return self.pool.getconn()

    def _put_connection(self, conn):
        self.pool.putconn(conn)

    @contextmanager
    def _transaction(self, cursor_factory=None):
        conn = self.pool.getconn()
        try:
            yield conn, conn.cursor(cursor_factory=cursor_factory)
            conn.commit()
        except Exception as ex:
            print(ex)
            conn.rollback()
        finally:
            self.pool.putconn(conn)

    @exception_decorator
    def set_schema_if_not_exists(self):
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema))
            )

    @exception_decorator
    def create_table_if_not_exists(self, table, event_ts, created_ts, entities: dict):
        with self._transaction() as (conn, cursor):
            cols = list(entities.keys()) + [event_ts, created_ts]
            query = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {table} ( \
                    {entities}, \
                    {event_ts} varchar(255), \
                    {created_ts} varchar(255), \
                    PRIMARY KEY({cols}) \
                );"
            ).format(
                table=sql.Identifier(self.schema, table),
                entities=sql.SQL(
                    sql.SQL(', ')
                    .join(
                        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(type))
                        for col, type in entities.items()
                    )
                    .as_string(conn)
                ),
                event_ts=sql.Identifier(event_ts),
                created_ts=sql.Identifier(created_ts),
                cols=sql.SQL(', ').join(sql.Identifier(col) for col in cols),
            )
            cursor.execute(query)

    @exception_decorator
    def get_source(self):
        with self._get_cursor() as cursor:
            cursor.execute("SELECT * FROM public.xxx")
            id = cursor.fetchone()

    @exception_decorator
    def get_example(self, field_of_activity, subject_area, column_type, value):
        """
        column_type: possible values = ['job_id', 'job_title']
        value: value of the column_type
        """
        if column_type not in ['job_id', 'job_title']:
            raise ValueError('Parameter column_type is not valid!')
        conn = self._get_connection()
        cur = self._get_dict_cursor(conn)
        query = sql.SQL(
            "SELECT * FROM {}.berufenet \
                WHERE field_of_activity = {} \
                and subject_area = {} \
                and {} = {};"
        ).format(
            *map(sql.Identifier, (self.schema,)),
            *map(sql.Literal, (field_of_activity, subject_area)),
            *map(sql.Identifier, (column_type,)),
            *map(sql.Literal, (value,)),
        )
        cur.execute(query)
        record = cur.fetchone()
        self._put_connection(conn)
        if record is None:
            raise MissingDataError('Select statement returned None.')
        return record

    @exception_decorator
    def delete_example(self, project_id):
        conn = self._get_connection()
        cur = conn.cursor()
        query = f'DELETE FROM {self.schema}.projects WHERE project_id = %s'
        params = (project_id,)
        cur.execute(query, params)
        conn.commit()
        self._put_connection(conn)
        return cur.statusmessage

    @exception_decorator
    def update_example(self, project_id, original_network_id):
        conn = self._get_connection()
        cur = conn.cursor()
        query = (
            f'UPDATE {self.schema}.projects '
            'SET original_network_id = %s '
            'WHERE project_id = %s'
        )
        params = (original_network_id, project_id)
        cur.execute(query, params)
        conn.commit()
        self._put_connection(conn)

    @exception_decorator
    def insert_example(self, edge_list, original_network_id):
        """
        edge_list: [(source_node: uuid, target_node: uuid)]
        original_network_id: uuid
        """
        conn = self._get_connection()
        cur = conn.cursor()
        args_str = ','.join(
            cur.mogrify("(%s, %s, %s)", (source_node, target_node, original_network_id)).decode(
                "utf-8"
            )
            for source_node, target_node in edge_list
        )
        cur.execute(
            f'INSERT INTO {self.schema}.original_edges '
            '(source_node, target_node, original_network_id) '
            f'VALUES {args_str} '
            'RETURNING original_edge_id;'
        )
        records = [next(iter(record)) for record in cur.fetchall() if len(record) > 0]
        conn.commit()
        self._put_connection(conn)
        return records

    @exception_decorator
    def simple_insert_example(self, designation, project_id):
        conn = self._get_connection()
        cur = conn.cursor()
        query = (
            f'INSERT INTO {self.schema}.predicted_network'
            '(designation, project_id) '
            'VALUES (%s, %s) '
            'RETURNING predicted_network_id;'
        )
        params = (designation, project_id)
        cur.execute(query, params)
        predicted_network_id = next(iter(cur.fetchone()), None)
        conn.commit()
        self._put_connection(conn)
        if predicted_network_id is not None:
            self.set_predicted_network_of_project(project_id, predicted_network_id)
        return predicted_network_id

    # ####################################
    # GENERIC_METHODS                    #
    # ####################################
    @exception_decorator
    def _check_if_row_exists(self, table: str, column: str, value: str) -> bool:
        """
        table: name of the database table in the stored schema
        column: name of the column to look for
        value: value of the column
        """
        conn = self._get_connection()
        cur = conn.cursor()
        query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {}.{} WHERE {} = {});").format(
            *map(sql.Identifier, (self.schema, table, column)), *map(sql.Literal, (value,))
        )
        cur.execute(query)
        record = cur.fetchone()
        self._put_connection(conn)
        return next(iter(record))


if __name__ == '__main__':
    import yaml

    with open("entity_store/entity_store_config.yaml", 'r') as file:
        try:
            config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc
    db = DatabaseConnector(config['postgres'])
    db.set_schema_if_not_exists()
    db.create_table_if_not_exists(
        table='test_table',
        event_ts='event_ts',
        created_ts='created_ts',
        entities={
            'id1': 'bigserial',
            'id2': 'bigserial',
        },
    )
