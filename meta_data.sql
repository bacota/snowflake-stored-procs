use schema bruce;


create or replace procedure create_fill_rates(
       SCHEMA_NAME VARCHAR,
       TABLE_NAME VARCHAR,
       EXPORT_DEST VARCHAR
) returns varchar
language javascript as
$$
    var sql, stmt, rs, select_list

    var fill_rate_table = `${SCHEMA_NAME}_${TABLE_NAME}_fill_rates`
    var src_table = `${SCHEMA_NAME}.${TABLE_NAME}`

    sql = "SELECT COLUMN_NAME " +
        " FROM INFORMATION_SCHEMA.COLUMNS " +
        " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
        " ORDER BY ORDINAL_POSITION"
    stmt = snowflake.createStatement({sqlText: sql, 
         binds: [SCHEMA_NAME, TABLE_NAME]
    })
    rs = stmt.execute()
    columns = []
    while (rs.next()) {
      columns.push(rs.getColumnValue(1))
    }

    sql = `CREATE  OR REPLACE TEMPORARY TABLE ${fill_rate_table} ` +
        `("aggregation_key" VARCHAR, "stat_name" VARCHAR,` +
        columns.map(c => `"${c}" float`).join(",") + ")"
    snowflake.createStatement({sqlText: sql, binds: []}).execute()

    select_list = ["'all'", "'count_filled'"].concat( columns.map(c => `count("${c}")`) )
    sql = `INSERT INTO ${fill_rate_table} SELECT ${select_list.join(',')} FROM ${src_table}`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()
    
    select_list = ["'all'", "'count_total'"].concat( columns.map(c => 'count(*)') )
    sql = `INSERT INTO ${fill_rate_table} SELECT ${select_list.join(',')} FROM ${src_table}`
    stmt = snowflake.createStatement({sqlText: sql, binds: []}).execute()

    select_list = ["'all'", "'percent_filled'"].concat( columns.map(c => `count("${c}")/count(*)`) )
    sql = `INSERT INTO ${fill_rate_table} SELECT ${select_list.join(',')} FROM ${src_table}`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()

    if (!EXPORT_DEST.startsWith("@")) {
       EXPORT_DEST = '@' + EXPORT_DEST
    }
    sql = `COPY INTO ${EXPORT_DEST} FROM ${fill_rate_table} file_Format=(type=csv compression=None) ` +
        `header=TRUE single=TRUE overwrite=TRUE`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()
$$;

--call bruce.create_fill_rates('GUARDANT', 'GUARDANT_ENCOUNTERS', '@TEST_STAGE/summaries/fill_rates.csv');



create or replace procedure create_row_counts(
       LOCATION VARCHAR,
       FILE_FORMAT VARCHAR,
       EXPORT_DEST VARCHAR,
       PATTERN VARCHAR
) returns varchar
language javascript execute as caller  as
$$
    var sql, stmt, rs, select_list;
    var rc_table = `tmp_row_counts`;
    var pattern = PATTERN ?  PATTERN = `${PATTERN}` : '';
    var ext_table = "TMP_EXTERNAL_FOR_ROW_COUNTS";

    sql = `CREATE OR REPLACE TEMPORARY TABLE ${rc_table} `+
        `("file_name" VARCHAR, "aggregation_key" VARCHAR, "num_rows" VARCHAR)`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()

    files = []
    sql = `list ${LOCATION} ${pattern}`
    rs = snowflake.createStatement({sqlText: sql, binds: []}).execute()
    while (rs.next()) {
       fpath = rs.getColumnValue(1)
       split_path = fpath.split("/")
       fname = split_path.pop()
       //fsize = rs.getColumnValue(2) 

       ext_sql = `CREATE OR REPLACE EXTERNAL TABLE ${ext_table} WITH LOCATION=${LOCATION} pattern='.*${fname}' file_format=(type=${FILE_FORMAT})`
       snowflake.createStatement({sqlText: ext_sql, binds: []}).execute()

       var rc_sql = `INSERT INTO ${rc_table} SELECT '${fname}', 'All', count(*) from ${ext_table}`
       snowflake.createStatement({sqlText: rc_sql, binds: []}).execute()
    }

    sql = `DROP TABLE ${ext_table}`
    snowflake.createStatement({sqlText: ext_sql, binds: []}).execute()

    if (!EXPORT_DEST.startsWith("@")) {
       EXPORT_DEST = '@' + EXPORT_DEST
    }
    sql = `COPY INTO ${EXPORT_DEST} FROM ${rc_table} file_format=(type=csv compression=None) ` +
        `header=TRUE single=TRUE overwrite=TRUE`
    snowflake.createStatement({sqlText: sql, binds: []}).execute()
$$;


--call bruce.create_row_counts('@GUARDANT.export_stage/Medical_Headers_DAT/',
--    'parquet', '@TEST_STAGE/summaries/row_counts.csv', '');
