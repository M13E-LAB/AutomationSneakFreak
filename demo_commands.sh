#!/bin/bash
# 🎥 Demo Script - Sneakers Kafka → Snowflake Pipeline

echo "🎬 === DEMO SNEAKERS KAFKA → SNOWFLAKE PIPELINE ==="
echo ""

echo "📋 1. Loading environment variables..."
source env_variables.txt
echo "✅ Variables loaded"
echo ""

echo "🐳 2. Building Docker image..."
docker build -t kafka-lab:latest .
echo "✅ Image built"
echo ""

echo "🚀 3. Starting Redpanda + Console..."
docker-compose up -d redpanda console
echo "✅ Redpanda started"
echo "🌐 Console available at: http://localhost:8080"
echo ""

echo "⏳ Waiting for Redpanda to start (10 seconds)..."
sleep 10

echo "📨 4. Creating Kafka topic..."
docker exec redpanda rpk topic create sneaker-orders --partitions 3
echo "✅ Topic 'sneaker-orders' created"
echo ""

echo "🎯 5. Starting Producer (generates sneaker orders)..."
docker-compose up -d producer
echo "✅ Producer started"
echo ""

echo "📥 6. Starting Consumer (loads to Snowflake)..."
docker-compose up -d consumer
echo "✅ Consumer started"
echo ""

echo "📊 7. Displaying real-time logs..."
echo "    Open http://localhost:8080 in your browser"
echo "   📍 Go to Topics → sneaker-orders"
echo ""
echo "🔥 Pipeline active! Press Ctrl+C to stop the logs"
echo ""

# Display real-time logs
docker-compose logs -f producer consumer
