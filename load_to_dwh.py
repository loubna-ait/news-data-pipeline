import pandas as pd
from sqlalchemy import create_engine
import os

# Récupère l’hôte PostgreSQL depuis la variable d’environnement (Docker = "postgres", local = "localhost")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Création de la connexion vers la base Data Warehouse "newsdw"
engine = create_engine(f"postgresql://postgres:postgres@{DB_HOST}:5432/newsdw")

try:
    # Chargement du fichier agrégé : nombre d’articles par source
    articles_by_source = pd.read_csv("data_lake/gold/articles_by_source.csv")
    articles_by_source.to_sql("articles_by_source", engine, if_exists="replace", index=False)

    # Chargement du fichier agrégé : nombre d’articles par date
    articles_by_date = pd.read_csv("data_lake/gold/articles_by_date.csv")
    articles_by_date.to_sql("articles_by_date", engine, if_exists="replace", index=False)

    # Chargement du fichier agrégé : nombre d’articles par langue
    articles_by_language = pd.read_csv("data_lake/gold/articles_by_language.csv")
    articles_by_language.to_sql("articles_by_language", engine, if_exists="replace", index=False)

    # Chargement du fichier : mots les plus fréquents
    top_words = pd.read_csv("data_lake/gold/top_words.csv")
    top_words.to_sql("top_words", engine, if_exists="replace", index=False)
    
    # By category
    articles_by_category = pd.read_csv("data_lake/gold/articles_by_category.csv")
    articles_by_category.to_sql("articles_by_category", engine, if_exists="replace", index=False)
    
    # By author
    articles_by_author = pd.read_csv("data_lake/gold/articles_by_author.csv")
    articles_by_author.to_sql("articles_by_author", engine, if_exists="replace", index=False)

    # Message si tout s’est bien passé
    print("Loaded to DWH OK")

except Exception as e:
    # Gestion d’erreur si PostgreSQL n’est pas démarré ou problème de connexion
    print(f"Erreur connexion DWH : {e}")
    print("Lance d'abord PostgreSQL avec docker-compose up db")