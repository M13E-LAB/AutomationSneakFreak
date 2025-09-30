# Snowflake ETL Starter

Quickstart to generate sample data with pandas/Faker and load into Snowflake using snowflake-connector-python.

## Prerequisites
- Python 3.9+
- pip available
- Snowflake account (account identifier: e.g. orgname-accountname)

## Install
```bash
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install "snowflake-connector-python[pandas]" pandas faker
```

## Configure env
```bash
export SNOWFLAKE_ACCOUNT="gupyoza-zq65095"
export SNOWFLAKE_USER="<your_user>"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_WAREHOUSE="TEACH_WH"   # optional
export SNOWFLAKE_DATABASE="RETAIL_LAB"  # optional
export SNOWFLAKE_ROLE="ACCOUNTADMIN"    # optional
```

## Run
```bash
python3 load_to_snowflake.py
```

## Verify in Snowsight
```sql
USE WAREHOUSE TEACH_WH;
USE DATABASE RETAIL_LAB;
USE SCHEMA STG;
SHOW TABLES;
SELECT COUNT(*) FROM ORDERS;
```

## Files
- data_generator.py — generate customers/inventory/orders dataframes
- setup_snowflake.sql — create warehouse/db/schemas
- load_to_snowflake.py — create tables and load dataframes via write_pandas

## Notes
- Columns are uppercased to match unquoted Snowflake identifiers.
- For perfect TIMESTAMP_TZ handling, set use_logical_type=True in write_pandas.
