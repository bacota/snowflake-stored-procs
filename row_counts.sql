use schema bruce;

create or replace procedure create_row_counts(
       LOCATION VARCHAR,
       FILE_FORMAT VARCHAR,
       EXPORT_DEST VARCHAR
) returns varchar
language javascript execute as caller  as
$$
    var sql;
    var rc_table = `tmp_row_counts`;

    sql = `CREATE OR REPLACE TEMPORARY TABLE ${rc_table} `+
        `AS SELECT regexp_replace(file_name, '.*/', '') AS file_name, 'All' AS aggregation_key, num_rows FROM ` + 
        `(SELECT metadata$filename AS file_name,  count(*) AS num_rows ` +
        `FROM ${LOCATION} (file_format => ${FILE_FORMAT}) group by metadata$filename)`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()


    if (!EXPORT_DEST.startsWith("@")) {
       EXPORT_DEST = '@' + EXPORT_DEST
    }
    sql = `COPY INTO ${EXPORT_DEST} FROM ${rc_table} file_format=(type=csv compression=None) ` +
        `header=TRUE single=TRUE overwrite=TRUE`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()
$$;


--call bruce.create_row_counts('@GUARDANT.export_stage/Medical_Headers_DAT/',
--   'parquet', '@TEST_STAGE/summaries/row_counts.csv');
