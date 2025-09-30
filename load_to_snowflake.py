import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from data_generator import (
    generate_customers,
    generate_inventory_data,
    generate_orders,
)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante: {name}")
    return value


def run_sql_file(cursor: snowflake.connector.cursor.SnowflakeCursor, sql_path: Path) -> None:
    with sql_path.open("r", encoding="utf-8") as f:
        statements = f.read().split(";")
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        cursor.execute(stmt)


def ensure_tables(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cursor.execute(
        """
        CREATE OR REPLACE TABLE RETAIL_LAB.STG.CUSTOMERS (
            CUSTOMER_ID NUMBER,
            NAME STRING,
            EMAIL STRING,
            CITY STRING,
            CHANNEL STRING
        )
        """
    )
    cursor.execute(
        """
        CREATE OR REPLACE TABLE RETAIL_LAB.STG.INVENTORY (
            PRODUCT_ID NUMBER,
            PRODUCT_NAME STRING,
            CATEGORY STRING,
            UNIT_PRICE NUMBER(38, 2),
            STOCK_QUANTITY NUMBER
        )
        """
    )
    cursor.execute(
        """
        CREATE OR REPLACE TABLE RETAIL_LAB.STG.ORDERS (
            ID NUMBER,
            PRODUCT_ID NUMBER,
            CUSTOMER_ID NUMBER,
            QUANTITY NUMBER,
            UNIT_PRICE NUMBER(38, 2),
            SOLD_AT TIMESTAMP_TZ
        )
        """
    )


def main() -> None:
    account = require_env("SNOWFLAKE_ACCOUNT")
    user = require_env("SNOWFLAKE_USER")
    password = require_env("SNOWFLAKE_PASSWORD")

    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "TEACH_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_LAB")
    role = os.getenv("SNOWFLAKE_ROLE")

    # Génération des DataFrames
    customers_df: pd.DataFrame = generate_customers(customers=100, seed=42)
    inventory_df: pd.DataFrame = generate_inventory_data(products=100, seed=42)
    orders_df: pd.DataFrame = generate_orders(
        orders=100, seed=42, inventory=inventory_df, customers=customers_df
    )

    # Harmoniser les noms de colonnes avec Snowflake (non quoted → UPPER)
    customers_df.columns = [str(c).upper() for c in customers_df.columns]
    inventory_df.columns = [str(c).upper() for c in inventory_df.columns]
    orders_df.columns = [str(c).upper() for c in orders_df.columns]

    # Connexion Snowflake
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        role=role,
    )
    try:
        cur = conn.cursor()
        try:
            # Initialisation (warehouse/db/schemas)
            sql_path = Path(__file__).with_name("setup_snowflake.sql")
            if sql_path.exists():
                run_sql_file(cur, sql_path)

            # Contexte
            if role:
                cur.execute(f"USE ROLE {role}")
            cur.execute(f"USE WAREHOUSE {warehouse}")
            cur.execute(f"USE DATABASE {database}")
            cur.execute("USE SCHEMA RETAIL_LAB.STG")

            # Tables
            ensure_tables(cur)

            # Chargement via write_pandas
            write_pandas(conn, customers_df, table_name="CUSTOMERS", database="RETAIL_LAB", schema="STG", auto_create_table=False)
            write_pandas(conn, inventory_df, table_name="INVENTORY", database="RETAIL_LAB", schema="STG", auto_create_table=False)
            write_pandas(conn, orders_df, table_name="ORDERS", database="RETAIL_LAB", schema="STG", auto_create_table=False)

            print("✅ Chargement Snowflake terminé (CUSTOMERS, INVENTORY, ORDERS → RETAIL_LAB.STG)")
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"❌ Erreur: {exc}")
        sys.exit(1)


