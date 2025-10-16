# Import necessary modules
import os  # To access environment variables
import sys  # To manage program exit codes
from pathlib import Path  # To manipulate file paths

import pandas as pd  # To manipulate DataFrames
import snowflake.connector  # Official Snowflake connector
from snowflake.connector.pandas_tools import write_pandas  # Tool to load DataFrames into Snowflake

# Import data generation functions from our local module
from data_generator import (
    generate_customers,     # Generates fake customer data
    generate_inventory_data,  # Generates inventory data (products)
    generate_orders,        # Generates fake orders
)


def require_env(name: str) -> str:
    """Utility function to retrieve a required environment variable.
    
    Args:
        name: Name of the environment variable to retrieve
        
    Returns:
        The value of the environment variable
        
    Raises:
        RuntimeError: If the variable is not defined or empty
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def run_sql_file(cursor: snowflake.connector.cursor.SnowflakeCursor, sql_path: Path) -> None:
    """Execute a SQL file by splitting its content into statements separated by ';'.
    
    Args:
        cursor: Snowflake cursor to execute queries
        sql_path: Path to the SQL file to execute
    """
    # Read the complete SQL file
    with sql_path.open("r", encoding="utf-8") as f:
        statements = f.read().split(";")
    
    # Execute each SQL statement separately
    for stmt in statements:
        stmt = stmt.strip()  # Remove leading/trailing spaces
        if not stmt:  # Ignore empty statements
            continue
        cursor.execute(stmt)


def ensure_tables(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    """Create or recreate necessary tables in the STG (staging) schema.
    
    Args:
        cursor: Snowflake cursor to execute DDL queries
    """
    # Customer table with basic information
    cursor.execute(
        """
        CREATE OR REPLACE TABLE RETAIL_LAB.STG.CUSTOMERS (
            CUSTOMER_ID NUMBER,      -- Unique customer identifier
            NAME STRING,             -- Customer full name
            EMAIL STRING,            -- Email address
            CITY STRING,             -- City of residence
            CHANNEL STRING           -- Acquisition channel (online, store, etc.)
        )
        """
    )
    
    # Product inventory table (sneakers)
    cursor.execute(
        """
        CREATE OR REPLACE TABLE RETAIL_LAB.STG.INVENTORY (
            PRODUCT_ID NUMBER,       -- Unique product identifier
            PRODUCT_NAME STRING,     -- Commercial product name
            BRAND STRING,            -- Brand (Nike, Adidas, etc.)
            MODEL STRING,            -- Specific model
            COLOR STRING,            -- Primary color
            MATERIAL STRING,         -- Primary material
            SIZE STRING,             -- Size (EU, US, UK)
            EDITION STRING,          -- Edition (Limited, Regular, etc.)
            CATEGORY STRING,         -- Category (Running, Basketball, etc.)
            UNIT_PRICE NUMBER(38, 2), -- Unit price with 2 decimals
            STOCK_QUANTITY NUMBER    -- Stock quantity
        )
        """
    )
    
    # Orders/sales table
    cursor.execute(
        """
        CREATE OR REPLACE TABLE RETAIL_LAB.STG.ORDERS (
            ID NUMBER,               -- Unique order identifier
            PRODUCT_ID NUMBER,       -- Reference to ordered product
            CUSTOMER_ID NUMBER,      -- Reference to customer
            QUANTITY NUMBER,         -- Ordered quantity
            UNIT_PRICE NUMBER(38, 2), -- Unit price at time of sale
            SOLD_AT TIMESTAMP_TZ     -- Sale date and time (with timezone)
        )
        """
    )


def main() -> None:
    """Main function that orchestrates the complete ETL process.
    
    1. Retrieves Snowflake connection parameters
    2. Generates fake data
    3. Connects to Snowflake
    4. Creates necessary tables
    5. Loads data into Snowflake
    """
    # === SNOWFLAKE CONFIGURATION ===
    # Retrieve required connection parameters from environment variables
    account = require_env("SNOWFLAKE_ACCOUNT")    # Ex: abc12345.eu-west-1
    user = require_env("SNOWFLAKE_USER")          # Snowflake username
    password = require_env("SNOWFLAKE_PASSWORD")  # Password

    # Optional parameters with default values
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "TEACH_WH")  # Compute warehouse
    database = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_LAB")  # Target database
    role = os.getenv("SNOWFLAKE_ROLE")                        # Snowflake role (optional)

    # === FAKE DATA GENERATION ===
    print("📊 Generating fake data...")
    # Generate 100 customers with fixed seed for reproducibility
    customers_df: pd.DataFrame = generate_customers(customers=100, seed=42)
    
    # Generate 100 sneaker products
    inventory_df: pd.DataFrame = generate_inventory_data(products=100, seed=42)
    
    # Generate 100 orders using generated customers and products
    orders_df: pd.DataFrame = generate_orders(
        orders=100, seed=42, inventory=inventory_df, customers=customers_df
    )

    # === COLUMN NORMALIZATION ===
    # Snowflake automatically converts non-quoted column names to UPPERCASE
    # We harmonize our DataFrames to avoid case issues
    print("🔄 Normalizing column names...")
    customers_df.columns = [str(c).upper() for c in customers_df.columns]
    inventory_df.columns = [str(c).upper() for c in inventory_df.columns]
    orders_df.columns = [str(c).upper() for c in orders_df.columns]

    # === SNOWFLAKE CONNECTION ===
    print("🔌 Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=account,      # Snowflake account identifier
        user=user,           # User
        password=password,   # Password
        warehouse=warehouse, # Compute warehouse to use
        database=database,   # Default database
        role=role,          # Role to assume (can be None)
    )
    
    try:
        # Create cursor to execute SQL queries
        cur = conn.cursor()
        try:
            # === SNOWFLAKE ENVIRONMENT INITIALIZATION ===
            print("🏗️ Initializing Snowflake environment...")
            # Execute setup script if it exists (create warehouse, database, schemas)
            sql_path = Path(__file__).with_name("setup_snowflake.sql")
            if sql_path.exists():
                run_sql_file(cur, sql_path)

            # === SNOWFLAKE CONTEXT CONFIGURATION ===
            # Define execution context (role, warehouse, database, schema)
            if role:
                cur.execute(f"USE ROLE {role}")  # Switch to specified role
            cur.execute(f"USE WAREHOUSE {warehouse}")      # Use compute warehouse
            cur.execute(f"USE DATABASE {database}")        # Use database
            cur.execute("USE SCHEMA RETAIL_LAB.STG")       # Use staging schema

            # === TABLE CREATION ===
            print("📋 Creating tables...")
            ensure_tables(cur)

            # === DATA LOADING ===
            print("⬆️ Loading data into Snowflake...")
            
            # Load CUSTOMERS table
            # write_pandas uses COPY INTO for efficient loading
            write_pandas(
                conn, customers_df, 
                table_name="CUSTOMERS", 
                database="RETAIL_LAB", 
                schema="STG", 
                auto_create_table=False  # Tables already exist
            )
            
            # Load INVENTORY table
            write_pandas(
                conn, inventory_df, 
                table_name="INVENTORY", 
                database="RETAIL_LAB", 
                schema="STG", 
                auto_create_table=False
            )
            
            # Load ORDERS table
            write_pandas(
                conn, orders_df, 
                table_name="ORDERS", 
                database="RETAIL_LAB", 
                schema="STG", 
                auto_create_table=False
            )

            print("✅ Snowflake loading completed (CUSTOMERS, INVENTORY, ORDERS → RETAIL_LAB.STG)")
            
        finally:
            # Close cursor to free resources
            cur.close()
    finally:
        # Close Snowflake connection
        conn.close()


# === PROGRAM ENTRY POINT ===
if __name__ == "__main__":
    """Main script entry point.
    
    Executes the main() function and handles global errors.
    In case of error, displays the message and exits with error code.
    """
    try:
        main()  # Execute main ETL process
    except Exception as exc:
        # Error handling: display and non-zero exit code
        print(f"❌ Error: {exc}")
        sys.exit(1)  # Exit code 1 indicates error