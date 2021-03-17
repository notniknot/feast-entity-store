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


def exception_decorator(wrapped_function):
    def _wrapper(*args, **kwargs):
        try:
            result = wrapped_function(*args, **kwargs)
        except Exception as error:
            logging.getLogger('postgres_connector').exception(
                'Exception occurred in %s.', wrapped_function.__name__
            )
            raise type(error)(f'Exception occurred in {wrapped_function.__name__}: {str(error)}')
        return result

    return _wrapper


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
        except Exception:
            logging.getLogger('postgres_connector').exception(
                'Exception occurred while connecting to the database'
            )

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
    def create_schema_if_not_exists(self):
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema))
            )

    @exception_decorator
    def create_log_table_if_not_exists(self):
        # ToDo: Syntax needs to be checked
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        "id" SERIAL PRIMARY KEY,
                        "started" TIMESTAMP WITHOUT TIME ZONE,
                        "ended" TIMESTAMP WITHOUT TIME ZONE,
                        "status" TEXT,
                        "entities" ARRAY,
                        "feature_table" VARCHAR(255),
                        "path" TEXT,
                    )
                    """
                )
            )

    @exception_decorator
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

    def create_view_if_not_exists(self, table_name, column_data: dict):
        # ToDo: Syntax needs to be checked
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL(
                    """
                    CREATE VIEW IF NOT EXISTS AS (
                        SELECT id, feature_table, MAX({event_ts}) as {event_ts}, {created_ts}, path FROM {entity_table}
                        GROUP BY id, feature_table, {created_ts}, path
                    )
                    """
                ).format(
                    event_ts=sql.Identifier(column_data['timestamp_column']),
                    created_ts=sql.Identifier(column_data['created_timestamp_column']),
                    entity_table=sql.Identifier(table_name),
                )
            )

            # https://stackoverflow.com/a/49148545
            query = sql.SQL(
                """
                IF NOT EXISTS(SELECT 'view exists' FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = N{view_name} AND TABLE_SCHEMA = {schema})
                    BEGIN
                        DECLARE @v_ViewCreateStatement VARCHAR(MAX) = '
                            CREATE VIEW {schema}.{view_name} AS
                                SELECT id, feature_table, MAX({event_ts}) as {event_ts}, {created_ts}, path FROM {entity_table}
                                GROUP BY id, feature_table, {created_ts}, path'
                        EXEC (@v_ViewCreateStatement)
                    END
                """
            ).format(
                view_name=sql.Identifier(f'max_{table_name}'),
                schema=sql.Identifier(self.schema),
                event_ts=sql.Identifier(column_data['timestamp_column']),
                created_ts=sql.Identifier(column_data['created_timestamp_column']),
                entity_table=sql.Identifier(table_name),
            )

    @exception_decorator
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

    @exception_decorator
    def add_log(self, data):
        # ToDo: Syntax needs to be checked
        with self._transaction() as (_, cursor):
            cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO jobs (started, ended, status, entities, feature_table, path)
                    VALUES({started}, {ended}, {status}, {entities}, {feature_table}, {path})
                    """
                ).format(
                    started=sql.Literal(),
                    ended=sql.Literal(),
                    status=sql.Literal(),
                    entities=sql.Literal(),
                    feature_table=sql.Literal(),
                    path=sql.Literal(),
                )
            )

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
    db = PostgresConnector(config['postgres'])
    db.create_schema_if_not_exists()
    columns = db.get_columns('feast/offline/driver_info')
    # db.create_table_if_not_exists(
    #     table='test_table',
    #     event_ts='event_ts',
    #     created_ts='created_ts',
    #     entities={
    #         'id1': 'bigserial',
    #         'id2': 'bigserial',
    #     },
    # )
