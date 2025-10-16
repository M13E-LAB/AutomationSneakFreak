#!/usr/bin/env python3
"""
🚀 KAFKA PRODUCER - Real-time sneaker orders generator

This script simulates an e-commerce system that continuously generates sneaker
orders and sends them to a Kafka/Redpanda topic.

Flow:
1. Load reference data (customers + sneaker products)
2. Generate orders randomly and continuously
3. Send each order to Kafka
4. Repeat indefinitely (real-time simulation)

Usage: Launched via Docker in the demo pipeline
"""

# === IMPORTS ===
import json  # To serialize events to JSON
import os    # To read environment variables
import random  # To generate random data
import time    # For pauses between orders
from datetime import datetime, timezone  # For timestamps
from kafka import KafkaProducer  # Official Kafka client
import sys
import threading  # For metrics server
from prometheus_client import Counter, Histogram, Gauge, start_http_server  # Prometheus metrics
sys.path.append('/app')  # Allows importing from parent directory

# Import data generators from our local module
from data_generator import generate_customers, generate_inventory_data, generate_orders

# === PROMETHEUS METRICS ===
# Compteurs pour les événements
orders_sent_total = Counter('sneaker_orders_sent_total', 'Total orders sent to Kafka', ['brand', 'status'])
orders_sent_duration = Histogram('sneaker_orders_sent_duration_seconds', 'Time spent sending orders to Kafka')
kafka_errors_total = Counter('sneaker_kafka_errors_total', 'Total Kafka errors')
current_orders_per_minute = Gauge('sneaker_orders_per_minute', 'Current orders per minute rate')


def create_kafka_producer():
    """Create and configure Kafka producer to send events.
    
    Configuration optimized for reliability:
    - acks='all': Wait for confirmation from all replicas
    - retries=3: Retry 3 times on failure
    - max_in_flight=1: Only one request in flight (guarantees order)
    
    Returns:
        KafkaProducer: Configured producer instance
    """
    # === CONNECTION CONFIGURATION ===
    # Get parameters from environment variables
    bootstrap_host = os.getenv('KAFKA_BOOTSTRAP_HOST', 'localhost')  # Kafka/Redpanda host
    bootstrap_port = os.getenv('KAFKA_BOOTSTRAP_PORT', '19092')      # Redpanda port
    bootstrap_servers = f'{bootstrap_host}:{bootstrap_port}'
    
    print(f"🔌 Connecting to Kafka: {bootstrap_servers}")
    
    # === KAFKA PRODUCER CREATION ===
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],  # Kafka cluster address
        
        # === SERIALIZERS ===
        # Convert Python objects to bytes for Kafka
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Events → JSON
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,  # Keys → String
        
        # === RELIABILITY CONFIGURATION ===
        acks='all',  # Wait for confirmation from ALL replicas (max reliability)
        retries=3,   # Number of retry attempts on network failure
        max_in_flight_requests_per_connection=1  # Only one request at a time (preserves order)
    )
    return producer


def generate_sneaker_order_event(customers_df, inventory_df):
    """Generate a realistic sneaker order event with consistent data.
    
    Simulates a real e-commerce order by:
    1. Randomly selecting a customer and product
    2. Generating realistic quantities (70% = 1 pair, 20% = 2 pairs, 10% = 3 pairs)
    3. Calculating total amount
    4. Creating a structured JSON event
    
    Args:
        customers_df: DataFrame of available customers
        inventory_df: DataFrame of available sneakers
        
    Returns:
        dict: Order event in JSON format
    """
    
    # === RANDOM DATA SELECTION ===
    # Choose a customer from the 100 available
    customer = customers_df.sample(1).iloc[0]
    
    # Choose a sneaker from the 100 available
    product = inventory_df.sample(1).iloc[0]
    
    # === ORDER DETAILS GENERATION ===
    # Realistic quantity distribution (most people buy 1 pair)
    quantity = random.choices(
        [1, 2, 3],                    # Possible quantities
        weights=[0.7, 0.2, 0.1]       # 70% = 1 pair, 20% = 2 pairs, 10% = 3 pairs
    )[0]
    
    # === EVENT CONSTRUCTION ===
    # Event structure compatible with streaming systems
    order_event = {
        # === EVENT METADATA ===
        "event_type": "order_created",  # Event type for routing
        "timestamp": datetime.now(timezone.utc).isoformat(),  # UTC ISO8601 timestamp
        "order_id": random.randint(10000, 99999),  # Unique order ID
        
        # === CUSTOMER INFORMATION ===
        "customer_id": int(customer['customer_id']),
        "customer_name": customer['name'],
        "customer_email": customer['email'],
        "customer_city": customer['city'],
        "channel": customer['channel'],  # Acquisition channel (online, store, etc.)
        
        # === SNEAKER PRODUCT INFORMATION ===
        "product_id": int(product['product_id']),
        "product_name": product['product_name'],  # Full commercial name
        "brand": product['brand'],                 # Nike, Adidas, Jordan, etc.
        "model": product['model'],                 # Air Max, Stan Smith, etc.
        "color": product['color'],                 # Primary color
        "material": product['material'],           # Leather, Mesh, Canvas, etc.
        "size": product['size'],                   # Size EU/US/UK
        "edition": product['edition'],             # Limited, Regular, Retro, etc.
        "category": product['category'],           # Running, Basketball, Lifestyle, etc.
        
        # === FINANCIAL INFORMATION ===
        "quantity": quantity,                                    # Number of pairs ordered
        "unit_price": float(product['unit_price']),            # Unit price
        "total_amount": float(product['unit_price']) * quantity, # Calculated total amount
        "currency": "EUR",                                     # Currency
        
        # === SYSTEM METADATA ===
        "source": "sneaker_store_app",  # Source application
        "version": "1.0"                # Event schema version
    }
    
    return order_event


def main():
    """Main producer loop - Generate and send orders continuously.
    
    Process:
    1. Load reference data (customers + sneakers)
    2. Create Kafka connection
    3. Infinite loop:
       - Generate random order
       - Send it to Kafka
       - Wait 2-8 seconds (realistic simulation)
       - Repeat
    
    This simulation generates ~720 orders/hour (one every 5s on average)
    """
    # === PROMETHEUS METRICS SERVER ===
    # Start metrics server on port 8001, listening on all interfaces
    start_http_server(8001, addr='0.0.0.0')
    print("📊 Prometheus metrics server started on port 8001")
    
    # === CONFIGURATION ===
    # Kafka topic name where to send events
    topic_name = os.getenv('KAFKA_TOPIC_NAME', 'sneaker-orders')
    
    print(f"🚀 Starting Sneakers Producer")
    print(f"📨 Topic: {topic_name}")
    
    # === REFERENCE DATA LOADING ===
    # This data serves as "catalog" to generate realistic orders
    print("📊 Loading sneakers data...")
    
    # Generate 100 fake customers (seed=42 for reproducibility)
    customers_df = generate_customers(customers=100, seed=42)
    
    # Generate 100 fake sneakers (same seed for consistency)
    inventory_df = generate_inventory_data(products=100, seed=42)
    
    print(f"✅ {len(customers_df)} customers and {len(inventory_df)} sneakers loaded")
    print(f"   Examples: {inventory_df['brand'].unique()[:5].tolist()}...")
    
    # === KAFKA PRODUCER CREATION ===
    producer = create_kafka_producer()
    
    # === MAIN GENERATION LOOP ===
    try:
        order_count = 0  # Counter to track number of orders sent
        start_time = time.time()  # For rate calculation
        
        print("\n🔄 Starting continuous order generation...")
        print("   (Ctrl+C to stop)\n")
        
        while True:  # Infinite loop - simulates real e-commerce system
            
            # === ORDER GENERATION ===
            # Create order event with random but consistent data
            order_event = generate_sneaker_order_event(customers_df, inventory_df)
            
            # === SEND TO KAFKA WITH METRICS ===
            try:
                with orders_sent_duration.time():  # Measure send duration
                    # Asynchronous send with partitioning key (order_id)
                    future = producer.send(
                        topic_name,                          # Destination topic
                        key=str(order_event['order_id']),    # Key for partitioning
                        value=order_event                    # JSON event
                    )
                    
                    # === WAIT FOR CONFIRMATION ===
                    # Block until write confirmation (reliability)
                    record_metadata = future.get(timeout=10)
                
                # === PROMETHEUS METRICS ===
                # Increment success counter by brand
                orders_sent_total.labels(brand=order_event['brand'], status='success').inc()
                
                # Update orders per minute gauge
                elapsed_minutes = (time.time() - start_time) / 60
                if elapsed_minutes > 0:
                    current_orders_per_minute.set(order_count / elapsed_minutes)
                
            except Exception as kafka_error:
                # === KAFKA ERROR HANDLING ===
                kafka_errors_total.inc()
                orders_sent_total.labels(brand=order_event['brand'], status='error').inc()
                print(f"❌ Kafka error: {kafka_error}")
                continue
            
            # === LOGGING AND STATISTICS ===
            order_count += 1
            
            # Display sent order details
            print(f"✅ [{order_count}] Order sent: {order_event['brand']} {order_event['model']} Size {order_event['size']} → {order_event['total_amount']}€")
            print(f"   👤 Customer: {order_event['customer_name']} ({order_event['customer_city']})")
            print(f"   📍 Kafka - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            # === REAL-TIME SIMULATION ===
            # Random pause to simulate natural order flow
            # In real e-commerce, orders don't arrive at fixed intervals
            sleep_time = random.uniform(2, 8)  # Between 2 and 8 seconds
            print(f"   ⏳ Waiting {sleep_time:.1f}s before next order...\n")
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        # Clean stop via Ctrl+C
        print(f"\n🛑 Producer stopped. Total orders sent: {order_count}")
        print(f"   📊 Average rate: ~{order_count/max(1, order_count*5/3600):.1f} orders/hour")
        
    except Exception as e:
        # Handle unexpected errors
        print(f"❌ Producer error: {e}")
        
    finally:
        # === CLEANUP ===
        # Clean producer shutdown to free resources
        producer.close()
        print("🔌 Producer closed")


# === ENTRY POINT ===
if __name__ == "__main__":
    """Producer entry point.
    
    Automatically launched by Docker Compose in the demo pipeline.
    Generates continuous sneaker orders to Kafka/Redpanda.
    
    Usage:
    - Via Docker: docker-compose up producer
    - Direct: python src/producer_order_status.py
    """
    main()

