"""
🚀 DAG Airflow - Rapport Quotidien Sneakers
Génère automatiquement des rapports d'analyse des ventes sneakers
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import snowflake.connector
import os
import json

# Configuration par défaut du DAG
default_args = {
    'owner': 'sneaker-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'daily_sneaker_report',
    default_args=default_args,
    description='Rapport quotidien des ventes sneakers',
    schedule_interval='0 8 * * *',  # Tous les jours à 8h du matin
    catchup=False,
    tags=['sneakers', 'analytics', 'daily-report']
)

def extract_snowflake_data(**context):
    """
    Extrait les données des dernières 24h depuis Snowflake
    """
    print("🔍 Extraction des données Snowflake...")
    
    # Configuration Snowflake
    conn = snowflake.connector.connect(
        account='gupyoza-zq65095',
        user='ANOUAR',
        password='Popez798@@@!!798!!@@',
        warehouse='TEACH_WH',
        database='RETAIL_LAB',
        schema='STG'
    )
    
    # Requête pour les données des dernières 24h
    query = """
    SELECT 
        BRAND,
        MODEL,
        SIZE,
        COUNT(*) as ORDERS_COUNT,
        SUM(TOTAL_AMOUNT) as TOTAL_REVENUE,
        AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE,
        DATE(PROCESSED_AT) as ORDER_DATE
    FROM STREAMING_ORDERS 
    WHERE PROCESSED_AT >= CURRENT_DATE - 1
    GROUP BY BRAND, MODEL, SIZE, DATE(PROCESSED_AT)
    ORDER BY TOTAL_REVENUE DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Sauvegarde des données pour les tâches suivantes
    df.to_json('/opt/airflow/reports/daily_data.json', orient='records')
    
    print(f"✅ {len(df)} lignes extraites et sauvegardées")
    return len(df)

def generate_analytics_report(**context):
    """
    Génère le rapport d'analyse des ventes
    """
    print("📊 Génération du rapport d'analyse...")
    
    # Lecture des données
    df = pd.read_json('/opt/airflow/reports/daily_data.json')
    
    if df.empty:
        print("⚠️ Aucune donnée trouvée pour les dernières 24h")
        return "no_data"
    
    # Calculs d'analyse
    total_orders = df['ORDERS_COUNT'].sum()
    total_revenue = df['TOTAL_REVENUE'].sum()
    avg_order_value = df['AVG_ORDER_VALUE'].mean()
    
    # Top marques
    top_brands = df.groupby('BRAND')['TOTAL_REVENUE'].sum().sort_values(ascending=False).head(5)
    
    # Top modèles
    top_models = df.groupby(['BRAND', 'MODEL'])['TOTAL_REVENUE'].sum().sort_values(ascending=False).head(5)
    
    # Tailles populaires
    popular_sizes = df.groupby('SIZE')['ORDERS_COUNT'].sum().sort_values(ascending=False).head(5)
    
    # Génération du rapport HTML
    html_report = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>📊 Rapport Quotidien Sneakers - {datetime.now().strftime('%Y-%m-%d')}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 8px; }}
            .metric {{ background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; }}
            .top-list {{ background: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 Rapport Quotidien Sneakers</h1>
            <p>Période: {datetime.now().strftime('%Y-%m-%d')} | Généré automatiquement par Airflow</p>
        </div>
        
        <div class="metric">
            <h2>📈 Métriques Clés</h2>
            <p><strong>Total Commandes:</strong> {total_orders:,}</p>
            <p><strong>Chiffre d'Affaires:</strong> {total_revenue:,.2f}€</p>
            <p><strong>Panier Moyen:</strong> {avg_order_value:.2f}€</p>
        </div>
        
        <div class="top-list">
            <h2>🏆 Top 5 Marques (CA)</h2>
            <table>
                <tr><th>Marque</th><th>Chiffre d'Affaires</th></tr>
    """
    
    for brand, revenue in top_brands.items():
        html_report += f"<tr><td>{brand}</td><td>{revenue:,.2f}€</td></tr>"
    
    html_report += """
            </table>
        </div>
        
        <div class="top-list">
            <h2>👟 Top 5 Modèles (CA)</h2>
            <table>
                <tr><th>Marque</th><th>Modèle</th><th>Chiffre d'Affaires</th></tr>
    """
    
    for (brand, model), revenue in top_models.items():
        html_report += f"<tr><td>{brand}</td><td>{model}</td><td>{revenue:,.2f}€</td></tr>"
    
    html_report += """
            </table>
        </div>
        
        <div class="top-list">
            <h2>📏 Tailles Populaires</h2>
            <table>
                <tr><th>Taille</th><th>Nombre de Commandes</th></tr>
    """
    
    for size, orders in popular_sizes.items():
        html_report += f"<tr><td>{size}</td><td>{orders}</td></tr>"
    
    html_report += """
            </table>
        </div>
        
        <div class="metric">
            <p><em>Rapport généré automatiquement par le pipeline Airflow à {datetime.now().strftime('%H:%M:%S')}</em></p>
        </div>
    </body>
    </html>
    """
    
    # Sauvegarde du rapport
    report_filename = f"/opt/airflow/reports/sneaker_report_{datetime.now().strftime('%Y%m%d')}.html"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(html_report)
    
    print(f"✅ Rapport généré: {report_filename}")
    return report_filename

def create_summary_metrics(**context):
    """
    Crée des métriques de synthèse pour Prometheus/Grafana
    """
    print("📊 Création des métriques de synthèse...")
    
    # Lecture des données
    df = pd.read_json('/opt/airflow/reports/daily_data.json')
    
    if df.empty:
        return "no_data"
    
    # Calculs de métriques
    metrics = {
        'daily_total_orders': int(df['ORDERS_COUNT'].sum()),
        'daily_total_revenue': float(df['TOTAL_REVENUE'].sum()),
        'daily_avg_order_value': float(df['AVG_ORDER_VALUE'].mean()),
        'unique_brands_sold': len(df['BRAND'].unique()),
        'unique_models_sold': len(df.groupby(['BRAND', 'MODEL']).size()),
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'generated_at': datetime.now().isoformat()
    }
    
    # Sauvegarde des métriques
    metrics_filename = f"/opt/airflow/reports/daily_metrics_{datetime.now().strftime('%Y%m%d')}.json"
    with open(metrics_filename, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"✅ Métriques sauvegardées: {metrics_filename}")
    return metrics

# Définition des tâches
extract_data_task = PythonOperator(
    task_id='extract_snowflake_data',
    python_callable=extract_snowflake_data,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_analytics_report',
    python_callable=generate_analytics_report,
    dag=dag,
)

create_metrics_task = PythonOperator(
    task_id='create_summary_metrics',
    python_callable=create_summary_metrics,
    dag=dag,
)

# Tâche de notification (optionnelle)
notify_completion_task = BashOperator(
    task_id='notify_completion',
    bash_command='echo "📧 Rapport quotidien sneakers généré avec succès à $(date)"',
    dag=dag,
)

# Définition des dépendances
extract_data_task >> [generate_report_task, create_metrics_task] >> notify_completion_task
