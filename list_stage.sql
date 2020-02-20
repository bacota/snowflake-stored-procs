use schema bruce;

create or replace procedure list_stage(SCHEMA_NAME VARCHAR, STAGE_NAME VARCHAR)
returns varchar
language javascript
execute as caller
as $$
var sql = `list @${SCHEMA_NAME}.${STAGE_NAME}`
var stmt = snowflake.createStatement({
    sqlText: sql,
    binds: []
})
var rs = stmt.execute()
return_list = []
while (rs.next()) {
      var file = rs.getColumnValue(1)
      var size = rs.getColumnValue(2)
      return_list.push(`${file}:${size}`)
}
return return_list.join("\n")
$$

