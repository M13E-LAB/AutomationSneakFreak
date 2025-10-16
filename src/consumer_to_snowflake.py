#!/usr/bin/env python3
"""
📥 KAFKA CONSUMER - Real-time sneaker orders to Snowflake loader

This script consumes sneaker order events from Kafka/Redpanda and loads them
into Snowflake in real-time, creating a streaming data pipeline.

Flow:
1. Connect to Kafka and Snowflake
2. Listen for order events from the topic
3. Transform each event to DataFrame format
4. Load immediately into Snowflake STREAMING_ORDERS table
5. Repeat continuously (real-time ETL)

Usage: Launched via Docker in the demo pipeline
"""

# === IMPORTS ===
import json  # To deserialize JSON events from Kafka
import os    # To read environment variables
import sys   # For system operations
import time  # For timing calculations
from datetime import datetime  # For timestamp handling
from kafka import KafkaConsumer  # Official Kafka client
import pandas as pd  # For DataFrame operations
import snowflake.connector  # Snowflake database connector
from snowflake.connector.pandas_tools import write_pandas  # Efficient DataFrame loading
from prometheus_client import Counter, Histogram, Gauge, start_http_server  # Prometheus metrics

# === PROMETHEUS METRICS ===
# Compteurs pour les événements
orders_consumed_total = Counter('sneaker_orders_consumed_total', 'Total orders consumed from Kafka', ['brand', 'status'])
orders_loaded_total = Counter('sneaker_orders_loaded_snowflake_total', 'Total orders loaded to Snowflake', ['status'])
snowflake_load_duration = Histogram('sneaker_snowflake_load_duration_seconds', 'Time spent loading to Snowflake')
snowflake_errors_total = Counter('sneaker_snowflake_errors_total', 'Total Snowflake errors')
current_processing_rate = Gauge('sneaker_orders_processing_per_minute', 'Current processing rate per minute')


def create_kafka_consumer():
    """Create and configure Kafka consumer to receive events.
    
    Configuration optimized for real-time processing:
    - auto_offset_reset='latest': Start with new messages only
    - group_id: Enables load balancing across multiple consumers
    - enable_auto_commit: Automatically commit message offsets
    
    Returns:
        KafkaConsumer: Configured consumer instance
    """
    # === CONNECTION CONFIGURATION ===
    # Get Kafka connection parameters from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP', 'localhost:19092')  # Redpanda address
    topic_name = os.getenv('KAFKA_TOPIC_NAME', 'sneaker-orders')         # Topic to consume
    
    print(f"🔌 Connecting Consumer to Kafka: {bootstrap_servers}")
    print(f"📥 Topic: {topic_name}")
    
    # === KAFKA CONSUMER CREATION ===
    consumer = KafkaConsumer(
        topic_name,  # Topic to subscribe to
        bootstrap_servers=[bootstrap_servers],  # Kafka cluster address
        
        # === DESERIALIZERS ===
        # Convert bytes from Kafka back to Python objects
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # JSON → Python dict
        key_deserializer=lambda k: k.decode('utf-8') if k else None,  # String keys
        
        # === CONSUMER GROUP CONFIGURATION ===
        group_id='sneaker-orders-consumer-group',  # Consumer group for load balancing
        auto_offset_reset='latest',    # Start with new messages (not historical)
        enable_auto_commit=True,       # Automatically commit processed messages
        auto_commit_interval_ms=1000   # Commit every 1 second
    )
    
    return consumer


def create_snowflake_connection():
    """Create Snowflake database connection for real-time loading.
    
    Uses environment variables for secure credential management.
    Configures connection for the RETAIL_LAB.STG schema.
    
    Returns:
        snowflake.connector.SnowflakeConnection: Database connection
        
    Raises:
        ValueError: If required credentials are missing
    """
    # === CREDENTIAL RETRIEVAL ===
    # Get Snowflake connection parameters from environment
    account = os.getenv('SNOWFLAKE_ACCOUNT')      # Snowflake account identifier
    user = os.getenv('SNOWFLAKE_USER')            # Username
    password = os.getenv('SNOWFLAKE_PASSWORD')    # Password
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'TEACH_WH')  # Compute warehouse
    database = os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_LAB')  # Target database
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'STG')             # Target schema
    role = os.getenv('SNOWFLAKE_ROLE')                        # Optional role
    
    # === VALIDATION ===
    # Ensure required credentials are provided
    if not all([account, user, password]):
        raise ValueError("Missing Snowflake variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
    
    print(f"❄️ Connecting to Snowflake: {account}")
    
    # === CONNECTION CREATION ===
    conn = snowflake.connector.connect(
        account=account,      # Snowflake account (e.g., abc12345.eu-west-1)
        user=user,           # Database user
        password=password,   # User password
        warehouse=warehouse, # Compute warehouse for queries
        database=database,   # Default database
        schema=schema,       # Default schema
        role=role           # Role to assume (optional)
    )
    
    return conn


def ensure_streaming_table(conn):
    """Create the streaming orders table if it doesn't exist.
    
    Creates RETAIL_LAB.STG.STREAMING_ORDERS table with schema optimized
    for real-time order events from Kafka.
    
    Args:
        conn: Snowflake connection object
    """
    cursor = conn.cursor()
    
    # === TABLE CREATION SQL ===
    # Schema designed to match the order event structure from producer
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS RETAIL_LAB.STG.STREAMING_ORDERS (
        -- Event metadata
        EVENT_TYPE STRING,           -- Type of event (order_created, etc.)
        TIMESTAMP TIMESTAMP_TZ,      -- Event timestamp with timezone
        ORDER_ID NUMBER,             -- Unique order identifier
        
        -- Customer information
        CUSTOMER_ID NUMBER,          -- Customer reference ID
        CUSTOMER_NAME STRING,        -- Customer full name
        CUSTOMER_EMAIL STRING,       -- Customer email address
        CUSTOMER_CITY STRING,        -- Customer city
        CHANNEL STRING,              -- Acquisition channel (online, store, etc.)
        
        -- Product information (sneakers)
        PRODUCT_ID NUMBER,           -- Product reference ID
        PRODUCT_NAME STRING,         -- Full product name
        BRAND STRING,                -- Sneaker brand (Nike, Adidas, etc.)
        MODEL STRING,                -- Sneaker model (Air Max, etc.)
        COLOR STRING,                -- Primary color
        MATERIAL STRING,             -- Material type (Leather, Mesh, etc.)
        SIZE STRING,                 -- Shoe size (EU/US/UK)
        EDITION STRING,              -- Edition type (Limited, Regular, etc.)
        CATEGORY STRING,             -- Category (Running, Basketball, etc.)
        
        -- Order financial details
        QUANTITY NUMBER,             -- Number of pairs ordered
        UNIT_PRICE NUMBER(38, 2),    -- Price per unit (2 decimal precision)
        TOTAL_AMOUNT NUMBER(38, 2),  -- Total order amount
        CURRENCY STRING,             -- Currency code (EUR, USD, etc.)
        
        -- System metadata
        SOURCE STRING,               -- Source system identifier
        VERSION STRING,              -- Event schema version
        PROCESSED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()  -- Processing timestamp
    )
    """
    
    # === TABLE CREATION ===
    cursor.execute(create_table_sql)
    cursor.close()
    print("✅ STREAMING_ORDERS table created/verified")


def process_order_event(event_data):
    """Transform Kafka order event into DataFrame for Snowflake loading.
    
    Converts the JSON event structure from Kafka into a pandas DataFrame
    with proper column names and data types for Snowflake.
    
    Args:
        event_data (dict): Order event from Kafka in JSON format
        
    Returns:
        pd.DataFrame: Single-row DataFrame ready for Snowflake loading
    """
    
    # === DATA EXTRACTION ===
    # Extract all fields from the event and map to Snowflake column names
    df_data = {
        # Event metadata
        'EVENT_TYPE': event_data.get('event_type'),
        'TIMESTAMP': event_data.get('timestamp'),
        'ORDER_ID': event_data.get('order_id'),
        
        # Customer information
        'CUSTOMER_ID': event_data.get('customer_id'),
        'CUSTOMER_NAME': event_data.get('customer_name'),
        'CUSTOMER_EMAIL': event_data.get('customer_email'),
        'CUSTOMER_CITY': event_data.get('customer_city'),
        'CHANNEL': event_data.get('channel'),
        
        # Product information
        'PRODUCT_ID': event_data.get('product_id'),
        'PRODUCT_NAME': event_data.get('product_name'),
        'BRAND': event_data.get('brand'),
        'MODEL': event_data.get('model'),
        'COLOR': event_data.get('color'),
        'MATERIAL': event_data.get('material'),
        'SIZE': event_data.get('size'),
        'EDITION': event_data.get('edition'),
        'CATEGORY': event_data.get('category'),
        
        # Financial information
        'QUANTITY': event_data.get('quantity'),
        'UNIT_PRICE': event_data.get('unit_price'),
        'TOTAL_AMOUNT': event_data.get('total_amount'),
        'CURRENCY': event_data.get('currency'),
        
        # System metadata
        'SOURCE': event_data.get('source'),
        'VERSION': event_data.get('version')
    }
    
    # === DATAFRAME CREATION ===
    # Create single-row DataFrame (each Kafka message = one order)
    df = pd.DataFrame([df_data])
    
    # === DATA TYPE CONVERSION ===
    # Convert timestamp string to pandas datetime for Snowflake compatibility
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    return df


def main():
    """Main consumer loop - Process Kafka events and load to Snowflake.
    
    Process:
    1. Create Kafka consumer and Snowflake connections
    2. Ensure target table exists
    3. Infinite loop:
       - Listen for Kafka messages
       - Transform each message to DataFrame
       - Load immediately to Snowflake
       - Repeat
    
    This creates a real-time streaming ETL pipeline with sub-second latency.
    """
    # === PROMETHEUS METRICS SERVER ===
    # Start metrics server on port 8002, listening on all interfaces
    start_http_server(8002, addr='0.0.0.0')
    print("📊 Prometheus metrics server started on port 8002")
    
    print("🚀 Starting Sneakers Consumer → Snowflake")
    
    # === CONNECTION SETUP ===
    # Create both Kafka and Snowflake connections
    consumer = create_kafka_consumer()
    snowflake_conn = create_snowflake_connection()
    
    # === TABLE PREPARATION ===
    # Ensure the target table exists with correct schema
    ensure_streaming_table(snowflake_conn)
    
    processed_count = 0  # Counter for processed messages
    start_time = time.time()  # For rate calculation
    
    # === MAIN PROCESSING LOOP ===
    try:
        print("👂 Listening for Kafka events...")
        
        # Infinite loop - process each message as it arrives
        for message in consumer:
            try:
                # === MESSAGE PROCESSING ===
                # Extract the order event from Kafka message
                event_data = message.value
                order_id = event_data.get('order_id')
                brand = event_data.get('brand', 'unknown')
                
                # === PROMETHEUS METRICS ===
                # Count consumed message
                orders_consumed_total.labels(brand=brand, status='success').inc()
                
                # === LOGGING ===
                # Display received event details
                print(f"\n📥 Event received: Order {order_id}")
                print(f"   🏷️ {event_data.get('brand')} {event_data.get('model')} Size {event_data.get('size')}")
                print(f"   💰 {event_data.get('total_amount')}€")
                print(f"   👤 Customer: {event_data.get('customer_name')} ({event_data.get('customer_city')})")
                
                # === DATA TRANSFORMATION ===
                # Convert event to DataFrame format
                df = process_order_event(event_data)
                
                # === SNOWFLAKE LOADING WITH METRICS ===
                try:
                    with snowflake_load_duration.time():  # Measure load duration
                        # Load DataFrame directly into Snowflake using write_pandas
                        # This uses Snowflake's COPY INTO for efficient bulk loading
                        write_pandas(
                            snowflake_conn,              # Database connection
                            df,                          # DataFrame to load
                            table_name='STREAMING_ORDERS', # Target table
                            database='RETAIL_LAB',       # Target database
                            schema='STG',                # Target schema
                            auto_create_table=False,     # Table already exists
                            use_logical_type=True        # Fix timezone warning
                        )
                    
                    # === SUCCESS METRICS ===
                    orders_loaded_total.labels(status='success').inc()
                    
                except Exception as snowflake_error:
                    # === SNOWFLAKE ERROR HANDLING ===
                    snowflake_errors_total.inc()
                    orders_loaded_total.labels(status='error').inc()
                    print(f"❌ Snowflake error: {snowflake_error}")
                    continue
                
                # === SUCCESS TRACKING ===
                processed_count += 1
                
                # Update processing rate gauge
                elapsed_minutes = (time.time() - start_time) / 60
                if elapsed_minutes > 0:
                    current_processing_rate.set(processed_count / elapsed_minutes)
                
                print(f"✅ Loaded to Snowflake! Total processed: {processed_count}")
                
            except Exception as e:
                # === ERROR HANDLING ===
                # Log individual message errors but continue processing
                brand = 'unknown'
                try:
                    brand = message.value.get('brand', 'unknown') if message.value else 'unknown'
                except:
                    pass
                orders_consumed_total.labels(brand=brand, status='error').inc()
                print(f"❌ Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        # === GRACEFUL SHUTDOWN ===
        # Clean stop via Ctrl+C
        print(f"\n🛑 Consumer stopped. Total processed: {processed_count}")
        
    except Exception as e:
        # === UNEXPECTED ERROR HANDLING ===
        print(f"❌ Consumer error: {e}")
        
    finally:
        # === CLEANUP ===
        # Close connections to free resources
        consumer.close()
        snowflake_conn.close()
        print("🔌 Consumer closed")


# === ENTRY POINT ===
if __name__ == "__main__":
    """Consumer entry point.
    
    Automatically launched by Docker Compose in the demo pipeline.
    Processes sneaker orders from Kafka and loads them to Snowflake in real-time.
    
    Usage:
    - Via Docker: docker-compose up consumer
    - Direct: python src/consumer_to_snowflake.py
    """
    main()