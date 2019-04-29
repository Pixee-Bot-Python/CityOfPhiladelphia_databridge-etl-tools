# databridge-etl-tools

Command line tools to extract and load SQL and Carto tables.

## Usage
```bash
# Extract a table from Oracle SDE to S3
databridge_etl_tools extract \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string user/password@db_alias
    --s3_bucket s3_bucket

# Load a table from S3 to Carto
databridge_etl_tools cartoupdate \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection_string carto://user:apikey \
    --s3_bucket s3_bucket

# Load a table from S3 to Postgres
databridge_etl_tools load \
    --table_name li_appeals_type \
    --table_schema gis_lni \
    --connection-string postgresql://user:password@host:port \
    --s3_bucket s3_bucket
```

## Installation
```bash
pip install git+https://github.com/CityOfPhiladelphia/databridge-etl-tools#egg=databridge_etl_tools
```