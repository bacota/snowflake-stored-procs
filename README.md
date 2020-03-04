Some POC experiments with Snowflake stored procedures.

Every stored procedure returns a single string (well, Varchar). That's all they can do.  Bummer. And they can't even be called within an expression.  Double bummer.  A stored procedure *can* call another stored procedure, but the only way I can see for a stored procedure to communicate with a caller  is to insert some rows in a table for the calling stored procedure to read.  I didn't actually try that, though.


`max_col_lengths` takes a schema name and table name, and returns a list of columns in that table along with the maximum length of the column (or maximum value if the column is numeric).   This is useful information because the columns in our existing encounters and certified claims schemas typically have the maximum length allowed by Snowflake, rather than using sizes based ona data model.

`fill_rates` is similar to `max_col_lengths`, but produces the fill rate (percent of non-null values) for each column.  This is data that the data services team needs to provide to its customers.

`redshift_schema` produces a `CREATE TABLE` command that should be suitable for a redshift table.  The length of each column is determined by the data in the actual column, computed as in `max_col_lengths`.  This may be useful for moving data from Snowflake to Redshift.

`external_tables` creates an external table pointing to data in a given s3 bucket in either parquet or csv format and outputs the `CREATE TABLE` command.  It assumes that the data in the external table is structured identically to that of some given table internal to snowflake.  This may be useful for the Data Services team because we typically export data from Snowflake to S3 for archival purposes and may need a convenient way to look at the archived data in the future.

`list_stage` lists a the contents of an external stage.  To get this to work it was necessary to define the procedure with `execute as caller`.

`fill_rates` and `row_counts are a couple of stored procedures for generating files of fill_rates and manifests (lists of files with row counts).


