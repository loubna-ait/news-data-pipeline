# 📰 News Data Pipeline (ETL & Streaming)

Bienvenue dans le projet **News Data Pipeline** ! 🚀
Ce projet est une infrastructure complète d'ingestion, de traitement et de stockage de données (ETL/ELT) conçue pour extraire des articles d'actualité, les traiter en temps réel via Kafka, et les structurer dans un Data Warehouse selon l'architecture "Medallion" (Bronze, Silver, Gold). L'orchestration de l'ensemble du workflow est gérée par Apache Airflow.

---

## 🏗️ Architecture du Projet

Le pipeline repose sur les technologies suivantes :
- **Apache Kafka & Zookeeper** : Ingestion des données en streaming.
- **Apache Airflow** : Orchestration et planification des tâches batch.
- **MinIO** : Object Storage (Data Lake) pour le stockage brut.
- **PostgreSQL** : Data Warehouse pour le stockage analytique structuré.
- **Docker & Docker Compose** : Conteneurisation de l'ensemble de l'infrastructure.

### 🥇 Architecture Medallion

Les données traversent plusieurs étapes pour garantir leur qualité et leur pertinence métier :

1. **Scraping (Producer Kafka)** - `scraper.py`
   - Extraction des titres d'articles depuis différentes sources (BBC, Hespress).
   - Les données sont envoyées sous forme de messages JSON vers le topic Kafka `news-topic`.

2. **Couche Bronze (Raw Data)** - `consumer_bronze.py`
   - Le consumer écoute le topic Kafka en continu.
   - Il sauvegarde les données sous forme brute (sans altération) dans le Data Lake local (`data_lake/bronze/articles.json`) et sur MinIO.

3. **Couche Silver (Cleaned Data)** - `silver_cleaner.py`
   - Nettoyage des textes, suppression du HTML résiduel, normalisation des formats de dates, etc.
   - Les données nettoyées sont sauvegardées dans `data_lake/silver/silver_articles.json`.

4. **Vérifications de Qualité (Data Observability)** - `quality_checks.py`
   - Audit des données Silver. Les articles avec des titres manquants ou du contenu anormalement court sont flaggués pour éviter de polluer les analyses.

5. **Couche Gold (Aggregated Data)** - `gold_transformer.py`
   - Transformation finale et agrégation métier (par source, langue, date, catégorie).
   - Extraction des mots les plus fréquents (Word Count).

6. **Chargement Data Warehouse** - `load_to_dwh.py`
   - Les données Gold sont insérées dans les tables analytiques de PostgreSQL.

---

## 🚀 Prérequis

Avant de lancer le projet, assurez-vous d'avoir installé sur votre machine :
- **Docker** et **Docker Desktop**
- **Docker Compose**
- Python 3.9+ (Optionnel, uniquement si vous souhaitez exécuter les scripts hors Docker)

---

## 🛠️ Déploiement & Lancement

Le projet est entièrement conteneurisé. Pour démarrer toute l'infrastructure (Zookeeper, Kafka, PostgreSQL, Airflow, Consumer), exécutez la commande suivante à la racine du projet :

```bash
docker-compose up -d --build
```

### Accès aux Services

Une fois les conteneurs démarrés, vous pouvez accéder aux différentes interfaces :

*   **Apache Airflow (Orchestrateur)** : [http://localhost:8080](http://localhost:8080)
    *   *Identifiant* : `admin`
    *   *Mot de passe* : `admin`
*   **PostgreSQL (Data Warehouse)** : `localhost:5432`
    *   *Base de données* : `newsdw`
    *   *Utilisateur* : `postgres`
    *   *Mot de passe* : `postgres`

---

## ⏳ Orchestration avec Airflow

Connectez-vous à l'interface Airflow, cherchez le DAG nommé `news_pipeline` et activez-le (bouton on/off).

Ce DAG est programmé pour tourner de manière récurrente (ex: toutes les minutes ou toutes les heures). Il orchestre les étapes de traitement Batch de la manière suivante :

`Scraping (Producer) ➔ Silver ➔ Quality ➔ Gold ➔ Load to DWH`

*(Note : Le Consumer Kafka tourne en arrière-plan en continu pour ingérer les données dès que le Scraper les produit).*

---

## 📊 Données Générées

À la fin du pipeline, les données sont prêtes à être connectées à un outil de BI (comme PowerBI, Tableau ou Metabase). Elles sont disponibles dans les tables PostgreSQL suivantes :

*   `articles_by_source` : Nombre d'articles par source d'actualité.
*   `articles_by_date` : Volume de publication par date.
*   `articles_by_language` : Répartition par langue.
*   `articles_by_category` : Répartition par catégorie.
*   `articles_by_author` : Répartition par auteur.
*   `top_words` : Les mots les plus fréquents extraits des titres.

---
*Ce projet est réalisé dans le cadre d'un projet académique sur l'ingénierie des données.*
