from __future__ import annotations

import logging
from contextlib import contextmanager
from io import StringIO

import pandas as pd
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


class MissingDataError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)


class PostgresConnector:

    instance = None

    data_type_mapping = {
        'INT64': 'BIGINT',
        'BOOL': 'BOOLEAN',
    }

    @classmethod
    def get_instance(cls) -> PostgresConnector:
        if cls.instance is None:
            cls.instance = cls()
        return cls.instance

    def __init__(self, config):
        self.logger = logging.getLogger('postgres_connector')
        try:
            self.pool = SimpleConnectionPool(1, 10, **config)
            self.schema = 'entity_lookup'
        except Exception as ex:
            self.logger.exception('Exception occurred while connecting to the database')
            raise ex

    @contextmanager
    def _transaction(self, cursor_factory=None):
        conn = self.pool.getconn()
        try:
            yield conn, conn.cursor(cursor_factory=cursor_factory)
            conn.commit()
        except Exception as ex:
            self.logger.exception('Exception occurred during database transaction')
            conn.rollback()
            raise ex
        finally:
            self.pool.putconn(conn)

    def create_schema_if_not_exists(self):
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema))
            )

    def create_log_table_if_not_exists(self):
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {schema}.jobs (
                        "id" SERIAL PRIMARY KEY,
                        "started" TIMESTAMP WITHOUT TIME ZONE,
                        "ended" TIMESTAMP WITHOUT TIME ZONE,
                        "status" VARCHAR(255),
                        "status_msg" TEXT,
                        "entity_names" CHARACTER VARYING[],
                        "feature_table" VARCHAR(255),
                        "path" TEXT
                    )
                    """
                ).format(schema=sql.Identifier(self.schema))
            )

    def _create_entity_table_if_not_exists(
        self, table_name: str, entity_type: str, event_ts: str, created_ts: str
    ):
        with self._transaction() as (_, cursor):
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {table} (
                    "id" {entity_type},
                    "feature_table" VARCHAR(255),
                    {event_ts} TIMESTAMP WITHOUT TIME ZONE,
                    {created_ts} TIMESTAMP WITHOUT TIME ZONE,
                    "path" TEXT,
                    PRIMARY KEY({cols})
                );
                """
            ).format(
                table=sql.Identifier(self.schema, table_name),
                entity_type=sql.SQL(entity_type),
                event_ts=sql.Identifier(event_ts),
                created_ts=sql.Identifier(created_ts),
                cols=sql.SQL(', ').join(
                    map(sql.Identifier, ['id', 'feature_table', event_ts, created_ts])
                ),
            )
            cursor.execute(query)

    def create_entity_tables_if_not_exist(self, column_data: dict, parquet_path: str):
        created_ts = column_data['created_timestamp_column']
        event_ts = column_data['timestamp_column']
        table_names = {}
        for entity_name, entity_type in zip(
            column_data['entity_names'], column_data['entity_types']
        ):
            table_name = f'entity_{entity_name}'
            self._create_entity_table_if_not_exists(
                table_name=table_name,
                entity_type=self.data_type_mapping[entity_type.upper()],
                event_ts=event_ts,
                created_ts=created_ts,
            )
            table_names[table_name] = entity_name
        return table_names

    def create_view_if_not_exists(self, table_names_for_entities, column_data: dict):
        for table_name in table_names_for_entities.keys():
            with self._transaction() as (_, cursor):
                query = sql.SQL(
                    """
                    DO $$
                    BEGIN
                        CREATE VIEW {schema}.{view_name} AS
                            WITH groups AS (
                                SELECT id, feature_table, MAX({event_ts}) as {event_ts}, {created_ts}, path,
                                ROW_NUMBER() OVER(PARTITION BY id ORDER BY {created_ts} DESC, path DESC) AS rk
                                FROM {schema}.{entity_table}
                                GROUP BY id, feature_table, {created_ts}, path
                            )
                            SELECT * FROM groups WHERE rk = 1
                    EXCEPTION
                    WHEN SQLSTATE '42P07' THEN
                        NULL;
                    END; $$
                    """
                ).format(
                    view_name=sql.Identifier(f'max_{table_name}'),
                    schema=sql.Identifier(self.schema),
                    event_ts=sql.Identifier(column_data['timestamp_column']),
                    created_ts=sql.Identifier(column_data['created_timestamp_column']),
                    entity_table=sql.Identifier(table_name),
                )
                cursor.execute(query)

    def get_columns(self, path_extract: str):
        with self._transaction(RealDictCursor) as (_, cursor):
            query = sql.SQL(
                """-- Reduce to one data source
                WITH data_source_ltd AS (
                    select max(ds.id), en.name as entity_name, en.type as entity_type, ft.name as feature_table, ds.timestamp_column, ds.created_timestamp_column from public.data_sources ds
                    JOIN public.feature_tables ft ON ds.id = ft.batch_source_id
                    JOIN public.feature_tables_entities_v2 fte ON ft.id = fte.feature_table_id
                    JOIN public.entities_v2 en ON fte.entity_v2_id = en.id
                    JOIN public.projects pr ON ft.project_name = pr.name
                    where ds.config::json ->> 'file_url' like {path}
                    and ft.is_deleted = false
                    and pr.archived = false
                    GROUP BY en.name, en.type, ft.name, ds.timestamp_column, ds.created_timestamp_column
                )
                -- Reduce to one row
                SELECT array_agg(entity_name) as entity_names, array_agg(entity_type) as entity_types, feature_table, timestamp_column, created_timestamp_column FROM data_source_ltd
                GROUP BY feature_table, timestamp_column, created_timestamp_column;
                """
            ).format(path=sql.Literal(f'%{path_extract}%'))
            cursor.execute(query)
            return cursor.fetchone()

    def copy_into_table(self, table_names_for_entities: dict, df: pd.DataFrame):
        for table_name, entity_name in table_names_for_entities.items():
            columns = [
                col
                for col in df.columns
                if col not in table_names_for_entities.keys() and col != table_name
            ]
            df_view = df[columns]
            columns[columns.index(entity_name)] = 'id'
            with self._transaction() as (_, cursor):
                s = StringIO()
                df_view.to_csv(s, header=False, index=False, sep='\t')
                s.seek(0)
                cursor.copy_from(s, f'{self.schema}.{table_name}', sep='\t', columns=columns)

    def add_log(self, data: dict):
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO {schema}.jobs (started, ended, status, status_msg, entity_names, feature_table, path)
                    VALUES({started}, {ended}, {status}, {status_msg}, {entity_names}, {feature_table}, {path})
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    started=sql.Literal(data['started']),
                    ended=sql.Literal(data['ended']),
                    status=sql.Literal(data['status']),
                    status_msg=sql.Literal(data['status_msg']),
                    entity_names=sql.Literal(data['entity_names']),
                    feature_table=sql.Literal(data['feature_table']),
                    path=sql.Literal(data['path']),
                )
            )
