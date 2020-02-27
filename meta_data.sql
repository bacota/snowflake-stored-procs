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
    stmt = snowflake.createStatement({sqlText: sql, binds: []});
    stmt.execute();

    select_list = ["'all'", "'count_filled'"].concat( columns.map(c => `count("${c}")`) )
    sql = `INSERT INTO ${fill_rate_table} SELECT ${select_list.join(',')} FROM ${src_table}`
    stmt = snowflake.createStatement({sqlText: sql, binds: []})
    stmt.execute()
    
    select_list = ["'all'", "'count_total'"].concat( columns.map(c => 'count(*)') )
    sql = `INSERT INTO ${fill_rate_table} SELECT ${select_list.join(',')} FROM ${src_table}`
    stmt = snowflake.createStatement({sqlText: sql, binds: []})
    stmt.execute()

    select_list = ["'all'", "'percent_filled'"].concat( columns.map(c => `count("${c}")/count(*)`) )
    sql = `INSERT INTO ${fill_rate_table} SELECT ${select_list.join(',')} FROM ${src_table}`
    stmt = snowflake.createStatement({sqlText: sql, binds: []})
    stmt.execute()

    if (!EXPORT_DEST.startsWith("@")) {
       EXPORT_DEST = '@' + EXPORT_DEST
    }
    sql = `COPY INTO ${EXPORT_DEST} FROM ${fill_rate_table} file_Format=(type=csv compression=None) ` +
        `header=TRUE single=TRUE overwrite=TRUE`
    stmt = snowflake.createStatement({sqlText: sql, binds: []})
    stmt.execute()

$$;

call bruce.create_fill_rates('GUARDANT', 'GUARDANT_ENCOUNTERS', '@TEST_STAGE/fill_rates/');
