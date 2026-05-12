# News Data Pipeline

Ce projet est un pipeline d'ingestion et de traitement de données (ETL) qui extrait des articles d'actualité depuis différentes sources, les traite via Kafka, effectue un nettoyage et des vérifications de qualité, pour finalement agréger les données dans un Data Warehouse PostgreSQL. Le tout est orchestré par Apache Airflow.

## Architecture

1. **Scraper (Producer)** : `scraper.py` extrait les titres d'articles depuis la BBC et Hespress et les publie dans un topic Kafka (`news-topic`).
2. **Bronze (Consumer)** : `consumer_bronze.py` lit les messages de Kafka et les sauvegarde sous forme brute dans le Data Lake (`data_lake/bronze/articles.json`).
3. **Silver (Cleaner)** : `silver_cleaner.py` nettoie le contenu HTML et normalise les données (`data_lake/silver/silver_articles.json`).
4. **Quality Checks** : `quality_checks.py` vérifie la qualité des données (titre manquant, contenu court, etc.) et ajoute un flag de qualité.
5. **Gold (Transformer)** : `gold_transformer.py` agrège les données (par source, date, langue, catégorie) et extrait les mots les plus fréquents pour générer des fichiers finaux.
6. **Data Warehouse (Load)** : `load_to_dwh.py` charge les fichiers Gold dans les tables PostgreSQL.

## Prérequis

- Docker
- Docker Compose

## Déploiement

Le projet est entièrement conteneurisé. Pour démarrer toute l'infrastructure (Zookeeper, Kafka, PostgreSQL, Airflow) :

```bash
docker-compose up -d --build
```

### Orchestration avec Airflow

Une fois les conteneurs démarrés, Airflow va s'occuper de lancer automatiquement tout le workflow :
- Le DAG `news_pipeline` s'exécute à un intervalle horaire (`@hourly`).
- Le DAG orchestre chaque étape de manière séquentielle : `Scraping -> Silver -> Quality -> Gold -> DWH`.
- Lors de l'étape de Scraping, le Consumer Kafka est lancé en arrière-plan pendant que le Scraper s'exécute, puis il est arrêté automatiquement après avoir récupéré les données.

Vous pouvez accéder à l'interface de suivi d'Airflow via :
- **URL** : http://localhost:8080
- **Identifiant** : admin
- **Mot de passe** : admin

## Données Générées

Les données seront disponibles dans les tables PostgreSQL suivantes (base `newsdw`) :
- `articles_by_source`
- `articles_by_date`
- `articles_by_language`
- `articles_by_category`
- `articles_by_author`
- `top_words`
