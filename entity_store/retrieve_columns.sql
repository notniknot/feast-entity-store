-- Reduce to one data source
WITH data_source_ltd AS (
	select max(ds.id), en.name as entity_name, en.type as entity_type, ft.name as feature_table, ds.timestamp_column, ds.created_timestamp_column from public.data_sources ds
	JOIN public.feature_tables ft ON ds.id = ft.batch_source_id
	JOIN public.feature_tables_entities_v2 fte ON ft.id = fte.feature_table_id
	JOIN public.entities_v2 en ON fte.entity_v2_id = en.id
	JOIN public.projects pr ON ft.project_name = pr.name
	where ds.config::json ->> 'file_url' like '%feast/offline/driver_info%'
	and ft.is_deleted = false
	and pr.archived = false
	GROUP BY en.name, en.type, ft.name, ds.timestamp_column, ds.created_timestamp_column
)
-- Reduce to one row
SELECT array_agg(entity_name) as entity_names, array_agg(entity_type) as entity_types, feature_table, timestamp_column, created_timestamp_column FROM data_source_ltd
GROUP BY feature_table, timestamp_column, created_timestamp_column;