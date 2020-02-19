Some POC experiments with Snowflake stored procedures.

Every stored procedure returns a single string (well, Varchar). That's all they can do.  Bummer. And they can't even be called within an expression.  Double bummer.  A stored procedure *can* call another stored procedure, but the only way I can see for a stored procedure to communicate with an calling stored procedue is to produce some output in a table for the calling stored procedure to read.


`max\_col\_lengths` takes a schema name and table name, and returns a list of columns in that table along with the maximum length of the column (or maximum value if the column is numeric).   This is useful information because the columns in our existing encounters and certified claims schemas typically have the maximum length allowed by Snowflake, rather than using sizes based ona data model.

`fill\_rates` is similar to `max\_col\_lengths`, but produces the fill rate (percent of non-null values) for each column.  This is data that the data services team needs to provide to its customers.

`redshift\_schema` produces a `CREATE TABLE` command that should be suitable for a redshift table.  The length of each column is determined by the data in the actual column, computed as in `max\_col\_lengths`.  This may be useful for moving data from Snowflake to Redshift.

`external\_tables` creates an external table pointing to data in a given s3 bucket in either parquet or csv format and outputs the `CREATE TABLE` command.  It assumes that the data in the external table is structured identically to that of some given table internal to snowflake.  This may be useful for the Data Services team because we typically export data from Snowflake to S3 for archival purposes and may need a convenient way to look at the archived data in the future.

`list_stage` is more of a "proof of failure" than "proof of concept".  I wanted to list all files in an external stage to create a "Manifest" for our customers.  The only way I can find to get a list of files from within Snowflake is with the `LIST` query.  Unfortunately the stored procedure's query API does not accept `LIST` queries.


