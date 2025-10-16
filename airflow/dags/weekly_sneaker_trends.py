"""
📈 DAG Airflow - Analyse Hebdomadaire des Tendances Sneakers
Analyse les tendances et patterns sur 7 jours
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import snowflake.connector
import json
# Matplotlib imports (will be installed in Airflow container)
try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError:
    print("⚠️ Matplotlib not available, visual reports will be skipped")
    plt = None
    sns = None
import base64
from io import BytesIO

# Configuration par défaut
default_args = {
    'owner': 'sneaker-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# DAG hebdomadaire (tous les lundis à 9h)
dag = DAG(
    'weekly_sneaker_trends',
    default_args=default_args,
    description='Analyse hebdomadaire des tendances sneakers',
    schedule_interval='0 9 * * 1',  # Tous les lundis à 9h
    catchup=False,
    tags=['sneakers', 'analytics', 'weekly-trends']
)

def extract_weekly_data(**context):
    """
    Extrait les données des 7 derniers jours
    """
    print("🔍 Extraction des données hebdomadaires...")
    
    conn = snowflake.connector.connect(
        account='gupyoza-zq65095',
        user='ANOUAR',
        password='Popez798@@@!!798!!@@',
        warehouse='TEACH_WH',
        database='RETAIL_LAB',
        schema='STG'
    )
    
    # Données détaillées des 7 derniers jours
    query = """
    SELECT 
        BRAND,
        MODEL,
        SIZE,
        COLOR,
        MATERIAL,
        TOTAL_AMOUNT,
        QUANTITY,
        CUSTOMER_CITY,
        CHANNEL,
        DATE(PROCESSED_AT) as ORDER_DATE,
        HOUR(PROCESSED_AT) as ORDER_HOUR,
        PROCESSED_AT
    FROM STREAMING_ORDERS 
    WHERE PROCESSED_AT >= CURRENT_DATE - 7
    ORDER BY PROCESSED_AT DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Sauvegarde
    df.to_json('/opt/airflow/reports/weekly_data.json', orient='records', date_format='iso')
    
    print(f"✅ {len(df)} commandes extraites sur 7 jours")
    return len(df)

def analyze_trends(**context):
    """
    Analyse les tendances et patterns
    """
    print("📊 Analyse des tendances hebdomadaires...")
    
    df = pd.read_json('/opt/airflow/reports/weekly_data.json')
    
    if df.empty:
        return "no_data"
    
    # Conversion des dates
    df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE'])
    df['PROCESSED_AT'] = pd.to_datetime(df['PROCESSED_AT'])
    
    # Analyses
    trends = {}
    
    # 1. Évolution quotidienne
    daily_sales = df.groupby('ORDER_DATE').agg({
        'TOTAL_AMOUNT': 'sum',
        'QUANTITY': 'sum'
    }).reset_index()
    trends['daily_evolution'] = daily_sales.to_dict('records')
    
    # 2. Marques en croissance
    brand_trends = df.groupby(['ORDER_DATE', 'BRAND'])['TOTAL_AMOUNT'].sum().reset_index()
    brand_growth = {}
    for brand in df['BRAND'].unique():
        brand_data = brand_trends[brand_trends['BRAND'] == brand].sort_values('ORDER_DATE')
        if len(brand_data) >= 2:
            growth = ((brand_data['TOTAL_AMOUNT'].iloc[-1] - brand_data['TOTAL_AMOUNT'].iloc[0]) / 
                     brand_data['TOTAL_AMOUNT'].iloc[0] * 100)
            brand_growth[brand] = round(growth, 2)
    trends['brand_growth'] = brand_growth
    
    # 3. Heures de pointe
    hourly_orders = df.groupby('ORDER_HOUR')['QUANTITY'].sum().to_dict()
    trends['peak_hours'] = hourly_orders
    
    # 4. Géographie des ventes
    city_sales = df.groupby('CUSTOMER_CITY')['TOTAL_AMOUNT'].sum().sort_values(ascending=False).head(10).to_dict()
    trends['top_cities'] = city_sales
    
    # 5. Canaux de vente
    channel_performance = df.groupby('CHANNEL').agg({
        'TOTAL_AMOUNT': 'sum',
        'QUANTITY': 'sum'
    }).to_dict('index')
    trends['channel_performance'] = channel_performance
    
    # Sauvegarde des analyses
    with open('/opt/airflow/reports/weekly_trends.json', 'w') as f:
        json.dump(trends, f, indent=2, default=str)
    
    print("✅ Analyse des tendances terminée")
    return trends

def generate_visual_report(**context):
    """
    Génère un rapport visuel avec graphiques
    """
    print("📈 Génération du rapport visuel...")
    
    # Vérifier si matplotlib est disponible
    if plt is None:
        print("⚠️ Matplotlib non disponible, génération de rapport textuel uniquement")
        return "matplotlib_unavailable"
    
    # Lecture des données
    df = pd.read_json('/opt/airflow/reports/weekly_data.json')
    
    if df.empty:
        return "no_data"
    
    df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE'])
    
    # Configuration matplotlib
    try:
        plt.style.use('seaborn-v0_8')
    except:
        plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('📊 Analyse Hebdomadaire Sneakers', fontsize=16, fontweight='bold')
    
    # 1. Évolution quotidienne des ventes
    daily_sales = df.groupby('ORDER_DATE')['TOTAL_AMOUNT'].sum()
    axes[0, 0].plot(daily_sales.index, daily_sales.values, marker='o', linewidth=2)
    axes[0, 0].set_title('💰 Évolution Quotidienne du CA')
    axes[0, 0].set_ylabel('Chiffre d\'Affaires (€)')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # 2. Top marques par CA
    brand_sales = df.groupby('BRAND')['TOTAL_AMOUNT'].sum().sort_values(ascending=True).tail(8)
    axes[0, 1].barh(brand_sales.index, brand_sales.values, color='skyblue')
    axes[0, 1].set_title('🏆 Top Marques (CA)')
    axes[0, 1].set_xlabel('Chiffre d\'Affaires (€)')
    
    # 3. Distribution des tailles
    size_dist = df['SIZE'].value_counts().head(10)
    axes[1, 0].bar(size_dist.index, size_dist.values, color='lightgreen')
    axes[1, 0].set_title('📏 Distribution des Tailles')
    axes[1, 0].set_ylabel('Nombre de Commandes')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 4. Heures de pointe
    hourly_orders = df.groupby('ORDER_HOUR')['QUANTITY'].sum()
    axes[1, 1].plot(hourly_orders.index, hourly_orders.values, marker='s', color='orange')
    axes[1, 1].set_title('⏰ Commandes par Heure')
    axes[1, 1].set_xlabel('Heure')
    axes[1, 1].set_ylabel('Nombre de Commandes')
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Sauvegarde du graphique
    chart_filename = f"/opt/airflow/reports/weekly_charts_{datetime.now().strftime('%Y%m%d')}.png"
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✅ Graphiques sauvegardés: {chart_filename}")
    return chart_filename

# Définition des tâches
extract_weekly_task = PythonOperator(
    task_id='extract_weekly_data',
    python_callable=extract_weekly_data,
    dag=dag,
)

analyze_trends_task = PythonOperator(
    task_id='analyze_trends',
    python_callable=analyze_trends,
    dag=dag,
)

generate_visual_task = PythonOperator(
    task_id='generate_visual_report',
    python_callable=generate_visual_report,
    dag=dag,
)

# Dépendances
extract_weekly_task >> analyze_trends_task >> generate_visual_task
