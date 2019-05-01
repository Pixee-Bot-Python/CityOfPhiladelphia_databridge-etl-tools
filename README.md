# databridge-etl-tools

Command line tools to extract and load SQL and Carto tables.

## Requirements (w/o Docker)
- Python 3.5 +
- Pip
- Oracle 11g Client
- Postgres

## Requirements (w/ Docker)
- Docker
- Access to citygeo-oracle-instant-client S3 bucket

## Usage
```bash
# Extract a table from Oracle SDE to S3
databridge_etl_tools extract \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string user/password@db_alias \
    --s3_bucket s3_bucket \
    --s3_key s3_key

# Load a table from S3 to Carto
databridge_etl_tools cartoupdate \
    --table_name li_appeals_type \
    --connection_string carto://user:apikey \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key\
    --csv_s3_key csv_s3_key \
    --select_users select_users

# Load a table from S3 to Postgres
databridge_etl_tools load \
    --table_name li_appeals_type \
    --table_schema lni \
    --connection-string postgresql://user:password@host:port/db_name \
    --s3_bucket s3_bucket \
    --json_schema_s3_key json_schema_s3_key \
    --csv_s3_key csv_s3_key
```

| Flag                 | Help                                                                    |
|----------------------|-------------------------------------------------------------------------|
| --table_name         | The name of the table to extract or load to/from in a database or Carto |
| --table_schema       | The name of the schema (user) to extract or load to/from in a database  |
| --connection_string  | The connection string to a database or Carto                            |
| --s3_bucket          | The S3 bucket to fetch or load to                                       |
| --s3_key             | The S3 key to dump an extract to                                        |
| --json_schema_s3_key | The S3 key to fetch a JSON schema file                                  |
| --csv_s3_key         | The S3 key to fetch a CSV file                                          |
| --select_users       | The Carto users to grant select access to (comma separated ie. public,tileuser)                              |

## Installation
```bash
pip install git+https://github.com/CityOfPhiladelphia/databridge-etl-tools#egg=databridge_etl_tools
```