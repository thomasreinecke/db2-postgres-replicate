

Super simple node-based approach to replicate tables and/or views from DB2 to PostgreSQL. This process only works in this direction.

example configuration `.env`

```
# DB2 connectivity configuration
DB2_HOST=<Hostname or IP of your DB2 source server>
DB2_PORT=<Port of DB2, usually 50000>
DB2_DB=<Source Database name>
DB2_USER=<Your DB2 username>
DB2_PASSWORD=<Your DB2 password>
DB2_SSL_TRUSTSTORE=<Path of Truststore, optional>
DB2_SSL_PASSWORD=<Password of Truststore, option>

# POSTGRES connectivity configuration
POSTGRES_HOST=<Hostname or IP of your PostgreSQL source server>
POSTGRES_PORT=<Port of Postgres, usually 5432>
POSTGRES_USER=<Your Postgres username>
POSTGRES_PASSWORD=<Your Postgres password>
POSTGRES_DB=<Target database on your Postgres server>

# force the reset of the destination at all times (not recommended)
RESET=false
# number of records to be tranferred at once, the batch size
CHUNK_SIZE=5000

# Configuration of the replicated tables (comma-separated if multiple tables should be replicated)
# REPLICATED_TABLES="<schema>.<table>"
# REPLICATED_TABLES="<schema1>.<table1>,<schema2>.<table2>"

# Configuration of replicated views (comma-separated if multiple views should be replicated)
# REPLICATED_VIEWS="<schema>.<view>"
# REPLICATED_VIEWS="<schema1>.<view1>,<schema2>.<view2>"
```

