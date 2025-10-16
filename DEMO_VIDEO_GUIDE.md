# 🎥 Guide Démo Vidéo - Pipeline Sneakers Kafka → Snowflake

## 📋 **Résumé du Projet**
Pipeline temps réel qui génère des commandes sneakers, les envoie vers Kafka/Redpanda, et les charge automatiquement dans Snowflake.

---

## 🏗️ **Architecture Créée**

```
📱 Producer (Python) → 📨 Kafka/Redpanda → 🐍 Consumer (Python) → ❄️ Snowflake
```

### **Composants :**
- **Redpanda** : Kafka compatible (port 19092)
- **Console Web** : Interface Kafka (port 8080)
- **Producer** : Génère commandes sneakers
- **Consumer** : Charge vers Snowflake
- **Docker** : Orchestration complète

---

## 📁 **Structure des Fichiers Créés**

```
ETL Warehouse/
├── docker-compose.yml          # Orchestration Docker
├── Dockerfile                  # Image Python avec dépendances
├── data_generator.py           # Générateur données sneakers
├── load_to_snowflake.py        # Chargement batch initial
├── env_variables.txt           # Variables d'environnement
└── src/
    ├── producer_order_status.py    # Producer Kafka
    └── consumer_to_snowflake.py    # Consumer Snowflake
```

---

## 🐳 **1. Configuration Docker**

### **docker-compose.yml** (services principaux)
```yaml
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:latest
    ports:
      - "19092:19092"  # Kafka
      - "8080:8080"    # Console Web
  
  producer:
    image: kafka-lab:latest
    command: ["python", "-u", "src/producer_order_status.py"]
  
  consumer:
    image: kafka-lab:latest
    command: ["python", "-u", "src/consumer_to_snowflake.py"]
```

### **Dockerfile**
```dockerfile
FROM python:3.11-slim
RUN pip install kafka-python snowflake-connector-python pandas faker
WORKDIR /app
COPY . /app
```

---

## 🎯 **2. Générateur de Données Sneakers**

### **Marques et Modèles** (dans data_generator.py)
```python
SNEAKER_BRANDS = [
    ("Nike", ["Air Max", "Air Force 1", "Dunk", "Jordan"]),
    ("Adidas", ["Stan Smith", "Superstar", "Yeezy", "Ultra Boost"]),
    ("New Balance", ["990", "574", "327", "2002R"]),
    # ... 8 marques au total
]
```

### **Exemple de Produit Généré**
```json
{
  "product_name": "Nike Air Max 90 White Leather - Size 42",
  "brand": "Nike",
  "model": "Air Max 90",
  "color": "White",
  "size": "42",
  "unit_price": 120.50
}
```

---

## 📨 **3. Producer Kafka (src/producer_order_status.py)**

### **Code Principal**
```python
def generate_sneaker_order_event(customers_df, inventory_df):
    customer = customers_df.sample(1).iloc[0]
    product = inventory_df.sample(1).iloc[0]
    
    order_event = {
        "event_type": "order_created",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": random.randint(10000, 99999),
        
        # Info client
        "customer_name": customer['name'],
        "customer_email": customer['email'],
        
        # Info sneaker
        "brand": product['brand'],
        "model": product['model'],
        "size": product['size'],
        "unit_price": float(product['unit_price']),
        "total_amount": float(product['unit_price']) * quantity
    }
    return order_event
```

### **Envoi vers Kafka**
```python
producer = KafkaProducer(
    bootstrap_servers=['redpanda:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

future = producer.send('sneaker-orders', value=order_event)
```

---

## 📥 **4. Consumer Snowflake (src/consumer_to_snowflake.py)**

### **Lecture Kafka**
```python
consumer = KafkaConsumer(
    'sneaker-orders',
    bootstrap_servers=['redpanda:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    event_data = message.value
    # Traitement...
```

### **Chargement Snowflake**
```python
df = pd.DataFrame([event_data])
write_pandas(
    snowflake_conn, 
    df, 
    table_name='STREAMING_ORDERS',
    database='RETAIL_LAB',
    schema='STG'
)
```

---

## 🚀 **5. Commandes de Démonstration**

### **Setup Initial**
```bash
# 1. Charger variables d'environnement
source env_variables.txt

# 2. Construire l'image Docker
docker build -t kafka-lab:latest .

# 3. Démarrer Redpanda + Console
docker-compose up -d redpanda console
```

### **Créer le Topic**
```bash
docker exec redpanda rpk topic create sneaker-orders --partitions 3
```

### **Lancer le Pipeline**
```bash
# 4. Démarrer le producer (génère commandes)
docker-compose up -d producer

# 5. Démarrer le consumer (charge Snowflake)
docker-compose up -d consumer
```

### **Monitoring en Live**
```bash
# Voir les logs en temps réel
docker-compose logs -f producer consumer

# Vérifier les topics Kafka
docker exec redpanda rpk topic list
```

---

## 🔍 **6. Points de Vérification pour la Vidéo**

### **Console Web Redpanda** : http://localhost:8080
- **Topics** → sneaker-orders
- Voir les messages JSON en temps réel
- Partitions et offsets

### **Logs Producer**
```
✅ [1] Commande envoyée: Nike Air Max 90 Size 42 → 120.50€
   📍 Partition: 0, Offset: 0
```

### **Logs Consumer**
```
📥 Événement reçu: Order 12345
   🏷️ Nike Air Max 90 Size 42
   💰 120.50€
✅ Chargé dans Snowflake! Total traité: 1
```

### **Snowflake** : https://gupyoza-zq65095.snowflakecomputing.com
```sql
USE WAREHOUSE TEACH_WH;
USE DATABASE RETAIL_LAB;
USE SCHEMA STG;

-- Voir les commandes streaming
SELECT BRAND, MODEL, SIZE, TOTAL_AMOUNT, PROCESSED_AT 
FROM STREAMING_ORDERS 
ORDER BY PROCESSED_AT DESC 
LIMIT 10;
```

---

## 📊 **7. Données Exemple Générées**

### **Commandes Typiques**
- Nike Air Max 90 White Size 42 → 120.50€
- Adidas Yeezy Boost 350 Black Size 41 → 220.00€
- New Balance 990v5 Grey Size 43 → 180.75€

### **Fréquence**
- 1 commande toutes les 2-8 secondes
- Distribution réaliste des tailles (40-43 plus fréquentes)
- Prix variables selon marque et édition

---

## 🎬 **8. Script de Présentation Suggéré**

1. **Montrer l'architecture** (diagramme)
2. **Code du producer** (génération événements)
3. **Lancer les commandes** Docker
4. **Console Redpanda** (messages temps réel)
5. **Logs des services** (producer + consumer)
6. **Snowflake** (données arrivées)
7. **Statistiques** (requêtes SQL)

---

## ⚡ **Commandes Rapides pour Live Demo**

```bash
# Reset complet
docker-compose down && docker-compose up -d redpanda console

# Créer topic
docker exec redpanda rpk topic create sneaker-orders --partitions 3

# Lancer pipeline
docker-compose up -d producer consumer

# Monitoring
docker-compose logs -f producer consumer
```



