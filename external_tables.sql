use schema bruce;

create or replace procedure make_external_table(SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR,
       EXT_TABLE_NAME VARCHAR, LOCATION VARCHAR, PATTERN VARCHAR, FILE_FORMAT VARCHAR)
returns varchar
language javascript as
$$

var prefix = "$"
if (FILE_FORMAT.toUpperCase().includes("CSV")) prefix = "c"

var select_list = []

var sql = "select COLUMN_NAME, DATA_TYPE "+
    " FROM INFORMATION_SCHEMA.COLUMNS " +
    " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
    " ORDER BY ORDINAL_POSITION "

var stmt = snowflake.createStatement({
         sqlText: sql,
         binds: [SCHEMA_NAME, TABLE_NAME]
    })
    
var rs = stmt.execute()
i=1
while (rs.next()) {
      var cname = rs.getColumnValue(1)
      var datatype = rs.getColumnValue(2)
      var column = `${cname} ${datatype} as (value:${prefix}${i}::${datatype})`
      select_list.push(column)
      i++
 }

var pattern = ''
if (PATTERN) {
   pattern = " pattern='"+PATTERN+"' "
}

ext_sql = "CREATE OR REPLACE EXTERNAL TABLE " + EXT_TABLE_NAME + "(" +
       select_list.join(",\n") +
       "\n) with location="+LOCATION + pattern + " file_format=("+FILE_FORMAT+")"

var ext_stmt = snowflake.createStatement({
         sqlText: ext_sql,
         binds: []
    })

ext_stmt.execute()

return ext_sql
$$;


EXAMPLE:
call make_external_table(
     'CHANCEY', 'CHANCEY_MEDICAL_HEADERS', 'EXT_MEDICAL_HEADERS',
     '@export_stage/header_201911/',
     '.*parquet', 'type=parquet'
);

