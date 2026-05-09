"""
dw_loader.py — Chargement Gold → Data Warehouse PostgreSQL
Couche : Data Warehouse (postgres_dw / news_dw)

Tables créées dans PostgreSQL :
    1. articles_by_source
    2. articles_by_language
    3. articles_by_day
    4. top_categories_by_source
    5. top_sources_by_language
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

from utils.minio_client import get_parquet_from_layer

logger = logging.getLogger(__name__)

# Connexion PostgreSQL — lue depuis la variable d'env Airflow
DW_CONN_STRING = os.getenv(
    'DW_CONN_STRING',
    'postgresql+psycopg2://datawarehouse:dw_password_2026@postgres_dw/news_dw'
)

# Mapping : fichier Gold dans MinIO → table PostgreSQL cible
GOLD_TABLES = {
    "articles_by_source/articles_by_source.parquet":             "articles_by_source",
    "articles_by_language/articles_by_language.parquet":         "articles_by_language",
    "articles_by_day/articles_by_day.parquet":                   "articles_by_day",
    "top_categories_by_source/top_categories_by_source.parquet": "top_categories_by_source",
    "top_sources_by_language/top_sources_by_language.parquet":   "top_sources_by_language",
}


def get_dw_engine():
    """Créer et retourner un moteur SQLAlchemy connecté au Data Warehouse."""
    return create_engine(DW_CONN_STRING)


def test_dw_connection():
    """Tester la connexion au DW. Lève une exception si la connexion échoue."""
    engine = get_dw_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("[DW] Connexion à postgres_dw réussie.")


def load_gold_to_dw():
    """
    Fonction principale appelée par le DAG 04_dw_loader.

    Pour chaque table Gold dans MinIO :
      1. Lire le fichier Parquet depuis le bucket 'gold'
      2. Écrire (ou remplacer) la table dans PostgreSQL news_dw
    """
    logger.info("=" * 60)
    logger.info("[DW] Démarrage du chargement Gold → Data Warehouse...")
    logger.info("=" * 60)

    # Vérifier la connexion avant de commencer
    test_dw_connection()

    engine = get_dw_engine()
    loaded = 0

    for object_name, table_name in GOLD_TABLES.items():
        try:
            # 1. Lire le Parquet Gold depuis MinIO
            logger.info(f"[DW] Lecture : gold/{object_name}")
            df = get_parquet_from_layer("gold", object_name)

            if df.empty:
                logger.warning(f"[DW] Parquet '{object_name}' vide, table ignorée.")
                continue

            # 2. Écrire dans PostgreSQL
            # if_exists='replace' : recrée la table à chaque run quotidien
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
                method="multi",  # Insertion par batch (plus rapide)
            )

            logger.info(
                f"[DW] ✔ '{table_name}' chargée : {len(df)} lignes dans news_dw"
            )
            loaded += 1

        except Exception as e:
            logger.error(f"[DW] ✘ Erreur chargement '{table_name}' : {e}")
            # On continue avec les autres tables

    logger.info("=" * 60)
    logger.info(f"[DW] Terminé : {loaded}/{len(GOLD_TABLES)} tables chargées.")
    logger.info("=" * 60)

    if loaded == 0:
        raise RuntimeError(
            "[DW] Aucune table chargée. "
            "Vérifier que le DAG 03_gold_analytics a bien tourné avant."
        )


if __name__ == "__main__":
    load_gold_to_dw()
