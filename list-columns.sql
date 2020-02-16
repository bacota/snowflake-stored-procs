use schema bruce;

create or replace procedure column_names(SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR)
returns varchar
language javascript as
$$
var sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH " +
    " FROM INFORMATION_SCHEMA.COLUMNS " +
    " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
    " ORDER BY ORDINAL_POSITION"
var stmt = snowflake.createStatement({
         sqlText: sql,
         binds: [SCHEMA_NAME, TABLE_NAME]
    })
var rs = stmt.execute()
columns = []
while (rs.next()) {
      cname = rs.getColumnValue(1)
      datatype = rs.getColumnValue(2)
      columns.push(cname + " " + datatype)
}
return columns.join("\n")
$$
