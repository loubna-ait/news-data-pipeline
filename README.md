# News Data Pipeline

Ce projet est un pipeline d'ingénierie de données (ETL) qui extrait des articles d'actualité en continu, les traite et les agrège dans une base de données relationnelle.

Le projet met en œuvre une **Architecture Medallion** (Bronze, Silver, Gold) pour garantir la transformation et la qualité des données à chaque étape.

## Architecture du Pipeline

Le flux de données suit plusieurs étapes orchestrées automatiquement :

1. **Extraction (Scraping)** : Récupération des articles (depuis BBC et Hespress) et envoi des données brutes vers un topic **Kafka**.
2. **Couche Bronze (Ingestion)** : Un consumer Kafka lit les données en temps réel et les sauvegarde dans un Data Lake (**MinIO**).
3. **Couche Silver (Nettoyage)** : Nettoyage des balises HTML, normalisation du texte, détection de la langue et application de règles de qualité (rejet des articles vides ou trop courts).
4. **Couche Gold (Agrégation)** : Transformation métier avec extraction des mots-clés les plus fréquents (NLP basique) et regroupement par source, date ou langue.
5. **Data Warehouse (Chargement)** : Chargement des données métier agrégées dans des tables **PostgreSQL**.

## Technologies Utilisées

- **Apache Kafka** : Streaming et ingestion de messages
- **MinIO** : Object Storage (Data Lake)
- **Python (Pandas, BeautifulSoup)** : Traitement, nettoyage et transformation
- **PostgreSQL** : Data Warehouse
- **Apache Airflow** : Orchestration des workflows
- **Docker & Docker Compose** : Déploiement et conteneurisation globale

## Guide de Démarrage

Prérequis : Avoir **Docker** et **Docker Compose** installés.

1. Démarrez l'ensemble de l'infrastructure (Zookeeper, Kafka, MinIO, Postgres, Airflow) :
```bash
docker-compose up -d --build
```

2. Accédez à l'interface **Airflow** pour suivre et lancer le DAG (`news_pipeline`) :
   - **URL** : http://localhost:8080
   - **Identifiant** : admin
   - **Mot de passe** : admin

*Le DAG est programmé pour exécuter toutes les étapes de manière séquentielle.*

## Résultats Générés

Une fois le pipeline exécuté, connectez-vous à la base PostgreSQL (`newsdw`) pour retrouver les tables prêtes à être analysées :
- `articles_by_source`
- `articles_by_language`
- `articles_by_date`
- `articles_by_category`
- `top_words`
