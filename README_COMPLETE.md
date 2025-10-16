# 🚀 ETL Warehouse - Pipeline Sneakers Temps Réel

> **Infrastructure complète de données pour e-commerce sneakers avec pipeline streaming, monitoring et rapports automatisés**

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)

## 🎯 **Vue d'ensemble**

Ce projet implémente un **pipeline ETL moderne** pour un e-commerce de sneakers, combinant :
- **Streaming temps réel** (Kafka → Snowflake)
- **Monitoring avancé** (Prometheus + Grafana)  
- **Rapports automatisés** (Airflow)
- **Données réalistes** (générateur de sneakers avec 8 marques)

## 🏗️ **Architecture**

```
📱 Producer Python → 📨 Kafka/Redpanda → 🐍 Consumer Python → ❄️ Snowflake
     ↓                    ↓                    ↓                    ↓
📊 Prometheus ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ←
     ↓                                                              ↓
📈 Grafana                                                   🔄 Airflow
```

## 🚀 **Démarrage Rapide**

### Prérequis
- Docker & Docker Compose
- Compte Snowflake actif
- 8GB RAM minimum

### 1. Configuration Snowflake
```bash
# Configurer les variables d'environnement
export SNOWFLAKE_ACCOUNT="votre-account"
export SNOWFLAKE_USER="votre-user"  
export SNOWFLAKE_PASSWORD="votre-password"
```

### 2. Lancement du pipeline complet
```bash
# Cloner le repository
git clone https://github.com/M13E-LAB/AutomationSneakFreak.git
cd AutomationSneakFreak

# Construire l'image Python
docker build -t kafka-lab:latest .

# Lancer l'infrastructure complète
docker-compose up -d

# Créer le topic Kafka
docker exec redpanda rpk topic create sneaker-orders --partitions 3

# Vérifier les logs
docker-compose logs -f producer consumer
```

## 🌐 **Services Disponibles**

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| **Grafana** | http://localhost:3000 | admin/admin123 | Dashboards & monitoring |
| **Airflow** | http://localhost:8081 | admin/admin123 | Orchestration & rapports |
| **Prometheus** | http://localhost:9090 | - | Métriques système |
| **Redpanda Console** | http://localhost:8080 | - | Interface Kafka |
| **Producer Metrics** | http://localhost:8001/metrics | - | Métriques producer |
| **Consumer Metrics** | http://localhost:8002/metrics | - | Métriques consumer |

## 📊 **Données Générées**

### Marques de Sneakers (réalistes)
- **Nike** : Air Max, Air Force 1, Dunk, Jordan, React, Zoom, Blazer, Cortez
- **Adidas** : Stan Smith, Superstar, Gazelle, Ultra Boost, NMD, Samba, Yeezy
- **New Balance** : 990, 574, 327, 2002R, 550, Fresh Foam, 9060
- **Converse** : Chuck Taylor, One Star, Pro Leather, Jack Purcell
- **Vans** : Old Skool, Authentic, Era, Sk8-Hi, Slip-On, Knu Skool
- **Puma** : Suede, Clyde, RS-X, Speedcat, Palermo, Easy Rider
- **Asics** : Gel-Lyte III, Gel-Kayano, Japan S, Mexico 66, Gel-Nimbus
- **Reebok** : Club C, Classic Leather, Pump, Instapump Fury, Question

### Exemple de commande générée
```json
{
  "event_type": "order_created",
  "timestamp": "2025-01-15T14:30:45.123Z",
  "order_id": 12345,
  "customer_name": "Marie Dubois",
  "customer_city": "Paris",
  "brand": "Nike",
  "model": "Air Max 90",
  "color": "White",
  "size": "42",
  "unit_price": 120.50,
  "total_amount": 241.00,
  "quantity": 2
}
```

## 📈 **Métriques Collectées**

### Producer (Port 8001)
- `sneaker_orders_sent_total` - Commandes envoyées par marque
- `sneaker_orders_sent_duration_seconds` - Latence d'envoi Kafka
- `sneaker_kafka_errors_total` - Erreurs Kafka
- `sneaker_orders_per_minute` - Taux de génération

### Consumer (Port 8002)  
- `sneaker_orders_consumed_total` - Commandes reçues par marque
- `sneaker_orders_loaded_snowflake_total` - Commandes chargées
- `sneaker_snowflake_load_duration_seconds` - Latence Snowflake
- `sneaker_orders_processing_per_minute` - Taux de traitement

## 🔄 **DAGs Airflow**

### 1. `daily_sneaker_report` (quotidien à 8h)
- Extraction des données Snowflake (24h)
- Génération de rapport HTML avec KPIs
- Métriques JSON pour Grafana
- Top marques, modèles, tailles populaires

### 2. `weekly_sneaker_trends` (hebdomadaire, lundi 9h)
- Analyse des tendances sur 7 jours
- Graphiques matplotlib (évolution, patterns)
- Détection des marques en croissance
- Analyse géographique et temporelle

## 🗄️ **Schéma Snowflake**

### Tables créées automatiquement
```sql
-- Données de référence (batch)
RETAIL_LAB.STG.CUSTOMERS     -- Clients
RETAIL_LAB.STG.INVENTORY     -- Inventaire sneakers  
RETAIL_LAB.STG.ORDERS        -- Commandes batch

-- Données streaming (temps réel)
RETAIL_LAB.STG.STREAMING_ORDERS  -- Commandes temps réel
```

### Exemple de requête d'analyse
```sql
-- Top 5 marques par CA (dernières 24h)
SELECT 
    BRAND,
    COUNT(*) as orders_count,
    SUM(TOTAL_AMOUNT) as revenue,
    AVG(TOTAL_AMOUNT) as avg_order_value
FROM RETAIL_LAB.STG.STREAMING_ORDERS 
WHERE PROCESSED_AT >= CURRENT_DATE - 1
GROUP BY BRAND
ORDER BY revenue DESC
LIMIT 5;
```

## 📁 **Structure du Projet**

```
ETL-Warehouse/
├── 📊 monitoring/
│   ├── prometheus.yml          # Config Prometheus
│   └── grafana/
│       ├── provisioning/       # Sources de données
│       └── dashboards/         # Dashboards JSON
├── 🔄 airflow/
│   ├── dags/                   # DAGs Python
│   ├── logs/                   # Logs Airflow
│   └── plugins/                # Plugins custom
├── 🐍 src/
│   ├── producer_order_status.py    # Générateur commandes
│   └── consumer_to_snowflake.py    # Chargeur Snowflake
├── 📋 reports/                 # Rapports générés
├── 🐳 docker-compose.yml      # Orchestration complète
├── 🔧 Dockerfile              # Image Python
├── 📊 data_generator.py       # Générateur données
└── ❄️ load_to_snowflake.py   # Chargement initial
```

## 🔧 **Configuration Avancée**

### Variables d'environnement
```bash
# Snowflake (requis)
SNOWFLAKE_ACCOUNT=gupyoza-zq65095
SNOWFLAKE_USER=ANOUAR
SNOWFLAKE_PASSWORD=***
SNOWFLAKE_WAREHOUSE=TEACH_WH
SNOWFLAKE_DATABASE=RETAIL_LAB
SNOWFLAKE_SCHEMA=STG

# Kafka
KAFKA_TOPIC_NAME=sneaker-orders
KAFKA_BOOTSTRAP=redpanda:9092

# Airflow
AIRFLOW__CORE__FERNET_KEY=***
```

### Personnalisation du générateur
```python
# Modifier data_generator.py pour :
- Ajouter de nouvelles marques
- Changer la distribution des prix  
- Modifier la fréquence de génération
- Personnaliser les tailles disponibles
```

## 📊 **Dashboards Grafana**

### Dashboard Principal : "Sneaker Pipeline Monitoring"
- **📨 Orders Sent Rate** - Taux d'envoi temps réel
- **📥 Orders Consumed Rate** - Taux de consommation  
- **📊 Total Orders** - Compteurs cumulés
- **⏱️ Latency Metrics** - Performance Snowflake
- **👟 Orders by Brand** - Répartition par marque
- **❌ Error Tracking** - Suivi des erreurs

## 🚨 **Monitoring & Alertes**

### Métriques clés à surveiller
- **Latence Snowflake** > 5 secondes
- **Erreurs Kafka** > 0
- **Taux de traitement** < 10 commandes/minute
- **Espace disque** Prometheus/Grafana

### Logs importants
```bash
# Logs producer/consumer
docker-compose logs -f producer consumer

# Logs Airflow
docker-compose logs -f airflow-scheduler airflow-webserver

# Métriques en direct
curl http://localhost:8001/metrics | grep sneaker
```

## 🔄 **Commandes Utiles**

```bash
# Redémarrage complet
docker-compose down && docker-compose up -d

# Reconstruction de l'image
docker build -t kafka-lab:latest . && docker-compose restart producer consumer

# Nettoyage des volumes
docker-compose down -v

# Backup des rapports
docker cp airflow-webserver:/opt/airflow/reports ./backup-reports

# Monitoring des ressources
docker stats

# Vérification des topics Kafka
docker exec redpanda rpk topic list
docker exec redpanda rpk topic describe sneaker-orders
```

## 🎯 **Cas d'Usage**

### 1. **E-commerce Analytics**
- Suivi temps réel des ventes
- Détection des tendances produits
- Optimisation des stocks
- Analyse comportementale clients

### 2. **Monitoring Opérationnel**  
- Performance du pipeline
- Détection d'anomalies
- Alertes automatiques
- Tableaux de bord executives

### 3. **Rapports Business**
- KPIs quotidiens automatisés
- Analyses hebdomadaires
- Comparaisons période sur période
- Insights géographiques

## 🛠️ **Développement**

### Ajouter une nouvelle métrique
```python
# Dans producer_order_status.py
new_metric = Counter('sneaker_new_metric_total', 'Description')
new_metric.labels(label='value').inc()
```

### Créer un nouveau DAG
```python
# Dans airflow/dags/
from airflow import DAG
from datetime import datetime

dag = DAG(
    'custom_sneaker_analysis',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1)
)
```

### Personnaliser les dashboards
1. Modifier `monitoring/grafana/dashboards/*.json`
2. Redémarrer Grafana : `docker-compose restart grafana`
3. Ou créer via l'interface web et exporter

## 🤝 **Contribution**

1. Fork le repository
2. Créer une branche feature (`git checkout -b feature/amazing-feature`)
3. Commit les changements (`git commit -m 'Add amazing feature'`)
4. Push vers la branche (`git push origin feature/amazing-feature`)
5. Ouvrir une Pull Request

## 📝 **Licence**

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## 🙏 **Remerciements**

- **Apache Kafka** pour le streaming
- **Snowflake** pour l'entrepôt de données  
- **Apache Airflow** pour l'orchestration
- **Prometheus & Grafana** pour le monitoring
- **Redpanda** pour l'interface Kafka simplifiée

---

**Développé avec ❤️ pour l'écosystème data moderne**
