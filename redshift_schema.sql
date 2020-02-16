use schema bruce;

create or replace procedure redshift_schema(SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR)
returns varchar
language javascript as
$$
var select_list = []
var columns = []
var column_types = {}
var sql = "select COLUMN_NAME, DATA_TYPE, NUMERIC_SCALE "+
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
      var scale = rs.getColumnValue(3)
      columns.push(cname)
      if (datatype == "TEXT") {
         column_types[cname] = 'VARCHAR'
         select_list.push(`MAX(LENGTH(${cname}))`)
      } else if (datatype == "NUMBER") {
         if (scale > 0) {
           column_types[cname] = 'DOUBLE'
           select_list.push("1")
         } else {
           column_types[cname] = 'NUMBER'
           select_list.push(`MAX(${cname})`)
         }
      } else if (datatype.startsWith('TIMESTAMP')) {
        column_types[cname] = 'TIMESTAMP'
        select_list.push("1")
      } else {
        column_types[cname] = datatype
        select_list.push("1")
      }
}

var query = `SELECT ${select_list.join(",")} FROM ${SCHEMA_NAME}.${TABLE_NAME}`
var max_stmt = snowflake.createStatement({
         sqlText: query,
         binds: []
    })
var rs = max_stmt.execute()
rs.next()
column_defs = []
for (i=0; i<columns.length; i++) {
    var cname = columns[i]
    var dtype = column_types[cname]
    var l = rs.getColumnValue(i+1)
    if (!l) {
       l = 1
    }
    if (dtype == 'VARCHAR') {
       dtype = `${dtype}(${l})`
    } else if (dtype == 'NUMBER') {
       var n = (""+l).length 
       dtype = `${dtype}(${n},0)`
    }
    column_defs.push(cname + " " + dtype)
}

return `CREATE TABLE ${TABLE_NAME} (\n${column_defs.join(",\n")});`

$$
