use schema bruce;

create or replace procedure max_column_lengths(SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR)
returns varchar
language javascript as
$$
var select_list = []
var columns = []
var sql = "select COLUMN_NAME, DATA_TYPE "+
    " FROM INFORMATION_SCHEMA.COLUMNS " +
    " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
    " ORDER BY ORDINAL_POSITION "
var stmt = snowflake.createStatement({
         sqlText: sql,
         binds: [SCHEMA_NAME, TABLE_NAME]
    })
var rs = stmt.execute()
while (rs.next()) {
      var cname = '"' + rs.getColumnValue(1) + '"'
      var datatype = rs.getColumnValue(2)
      if (datatype == "TEXT") {
         columns.push(cname)
         select_list.push(`MAX(LENGTH(${cname}))`)
      } if (datatype == "NUMBER") {
         columns.push(cname)
         select_list.push(`MAX(${cname})`)
      }
}

var query = `SELECT ${select_list.join(",")}  FROM ${SCHEMA_NAME}.${TABLE_NAME}`
var max_stmt = snowflake.createStatement({
         sqlText: query,
         binds: []
    })
var rs = max_stmt.execute()
results = []
rs.next()
for (i=0; i<columns.length; i++) {
    results.push(columns[i] + ": " + rs.getColumnValue(i+1))
}
return results.join("\n")
$$
