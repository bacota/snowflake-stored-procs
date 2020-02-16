use schema bruce;

create or replace procedure fill_rates(SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR)
returns varchar
language javascript as
$$
var select_list = []
var columns = []
var sql = "select COLUMN_NAME "+
    " FROM INFORMATION_SCHEMA.COLUMNS " +
    " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
    " ORDER BY ORDINAL_POSITION "
var stmt = snowflake.createStatement({
         sqlText: sql,
         binds: [SCHEMA_NAME, TABLE_NAME]
    })
var rs = stmt.execute()
while (rs.next()) {
      var cname = rs.getColumnValue(1)
      columns.push(cname)
      select_list.push("COUNT("+cname+")/COUNT(*)")
}

var query = "SELECT " + select_list.join(",") + " FROM " + SCHEMA_NAME + "." + TABLE_NAME
var max_stmt = snowflake.createStatement({
         sqlText: query,
         binds: [SCHEMA_NAME, TABLE_NAME]
    })
var rs = max_stmt.execute()
results = []
rs.next()
for (i=0; i<columns.length; i++) {
    results.push(columns[i] + ": " + rs.getColumnValue(i+1))
}
return results.join("\n")
$$
