# Snowflake ETL Starter

Projet de démarrage rapide pour générer des données d'exemple avec pandas/Faker et les charger dans Snowflake.

## Prérequis
- Python 3.9+ installé
- Compte Snowflake actif (identifiant: ex. orgname-accountname)
- Accès à un terminal/ligne de commande

## Étape 1: Installation des dépendances
Installez les packages Python nécessaires :
```bash
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install "snowflake-connector-python[pandas]" pandas faker
```

## Étape 2: Configuration Snowflake
Configurez vos variables d'environnement avec vos identifiants Snowflake :
```bash
export SNOWFLAKE_ACCOUNT="gupyoza-zq65095"
export SNOWFLAKE_USER="<votre_utilisateur>"
export SNOWFLAKE_PASSWORD="<votre_mot_de_passe>"
export SNOWFLAKE_WAREHOUSE="TEACH_WH"   # optionnel
export SNOWFLAKE_DATABASE="RETAIL_LAB"  # optionnel
export SNOWFLAKE_ROLE="ACCOUNTADMIN"    # optionnel
```
⚠️ Remplacez les valeurs entre `< >` par vos vraies informations.

## Étape 3: Préparation de l'environnement Snowflake
Exécutez le script SQL pour créer la structure nécessaire :
```sql
-- Exécutez setup_snowflake.sql dans Snowsight ou votre client SQL
```

## Étape 4: Génération et chargement des données
Lancez le script principal qui va :
- Générer des données fictives (clients, inventaire, commandes)
- Créer les tables dans Snowflake
- Charger les données
```bash
python3 load_to_snowflake.py
```

## Étape 5: Vérification dans Snowsight
Connectez-vous à Snowsight et vérifiez que les données ont été chargées :
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

