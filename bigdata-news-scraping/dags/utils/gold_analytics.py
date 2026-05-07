"""
gold_analytics.py — Transformation Silver -> Gold
Couche: Gold
-KPIs::
    1. Nombre d'articles par source
    2. Nombre d'articles par langue
    3. Nombre d'articles par jour
    4. Top catégories par source
    5. Top sources par langue


-Structure bucket gold:
    gold/articles_by_source/articles_by_source.parquet
    gold/articles_by_language/articles_by_language.parquet
    gold/articles_by_day/articles_by_day.parquet
    gold/top_categories_by_source/top_categories_by_source.parquet
    gold/top_sources_by_language/top_sources_by_language.parquet
"""

import pandas as pd
import logging
from datetime import datetime

from utils.minio_client import (
    list_objects,
    get_parquet_from_layer,
    save_parquet_to_layer,
)

logger = logging.getLogger(__name__)


#CHARGEMENT DES DONNÉES SILVER

def load_all_silver_data() -> pd.DataFrame:
    """
    Lit tous les fichiers Parquet présents dans le bucket Silver
    et les concatène en un seul DataFrame.
    """
    logger.info("[Gold] Chargement des données Silver...")

    silver_files = list_objects("silver")

    if not silver_files:
        raise ValueError(
            "[Gold] Aucun fichier Parquet trouvé dans le bucket Silver. "
            "Vérifier que le DAG 02_silver_cleaning a bien été exécuté."
        )

    dataframes = []
    for file_path in silver_files:
        try:
            df = get_parquet_from_layer("silver", file_path)
            dataframes.append(df)
            logger.info(f"[Gold] Chargé : {file_path} ({len(df)} lignes)")
        except Exception as e:
            logger.warning(f"[Gold] Impossible de lire {file_path} : {e}")
            continue

    if not dataframes:
        raise ValueError("[Gold] Aucune donnée Silver exploitable.")

    #Concaténer tous les DataFrames en un seul
    df_all = pd.concat(dataframes, ignore_index=True)

    #Supprimer les doublons éventuels basés sur url + source
    initial_count = len(df_all)
    df_all = df_all.drop_duplicates(subset=["url", "source"], keep="first")
    logger.info(
        f"[Gold] {initial_count} lignes chargées -> "
        f"{len(df_all)} après suppression des doublons."
    )

    return df_all


#KPI 1: ARTICLES PAR SOURCE

def compute_articles_by_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    -KPI 1: Nombre total d'articles collectés par source.

    -Colonnes résultantes:
        source        (str)  : nom de la source
        article_count (int)  : nombre d'articles
        avg_content_length (float) : longueur moyenne du contenu (si disponible)
    """
    logger.info("[Gold] Calcul KPI 1 : articles par source...")

    result = (
        df.groupby("source", as_index=False)
        .agg(
            article_count=("url", "count"),
        )
        .sort_values("article_count", ascending=False)
        .reset_index(drop=True)
    )

    #Ajouter la longueur moyenne du contenu si la colonne existe
    if "content" in df.columns:
        df["content_length"] = df["content"].fillna("").str.len()
        avg_len = (
            df.groupby("source")["content_length"]
            .mean()
            .round(1)
            .reset_index()
            .rename(columns={"content_length": "avg_content_length"})
        )
        result = result.merge(avg_len, on="source", how="left")

    result["computed_at"] = datetime.now().isoformat()

    logger.info(f"[Gold] KPI 1 : {len(result)} sources calculées.")
    return result


#KPI 2: ARTICLES PAR LANGUE

def compute_articles_by_language(df: pd.DataFrame) -> pd.DataFrame:
    """
    -KPI 2: Nombre total d'articles par langue détectée.

    -Colonnes résultantes:
        language      (str)  : code langue ISO (fr, en, ar, unknown...)
        article_count (int)  : nombre d'articles
        percentage    (float): part en % du total
    """
    logger.info("[Gold] Calcul KPI 2 : articles par langue...")

    if "language" not in df.columns:
        raise ValueError("[Gold] Colonne 'language' absente — vérifier la couche Silver.")

    #Remplacer les valeurs nulles par 'unknown'
    df["language"] = df["language"].fillna("unknown")

    result = (
        df.groupby("language", as_index=False)
        .agg(article_count=("url", "count"))
        .sort_values("article_count", ascending=False)
        .reset_index(drop=True)
    )

    #Calculer le pourcentage
    total = result["article_count"].sum()
    result["percentage"] = (result["article_count"] / total * 100).round(2)
    result["computed_at"] = datetime.now().isoformat()

    logger.info(f"[Gold] KPI 2 : {len(result)} langues calculées.")
    return result


#KPI 3: ARTICLES PAR JOUR

def compute_articles_by_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    -KPI 3: Nombre d'articles publiés par jour.
    -Permet de visualiser les tendances temporelles (line chart).

    -Colonnes résultantes:
        date          (str)  : date au format AAAA-MM-JJ
        article_count (int)  : nombre d'articles publiés ce jour-là
    """
    logger.info("[Gold] Calcul KPI 3 : articles par jour...")

    #Identifier la colonne de date disponible
    date_col = None
    for col in ["date_publication", "scraped_at", "date"]:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        raise ValueError(
            "[Gold] Aucune colonne de date trouvée (date_publication, scraped_at, date)."
        )

    #Convertir en datetime et extraire la date uniquement
    df["_date_parsed"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)

    #Exclure les lignes sans date valide
    df_valid = df.dropna(subset=["_date_parsed"]).copy()
    df_valid["date"] = df_valid["_date_parsed"].dt.strftime("%Y-%m-%d")

    result = (
        df_valid.groupby("date", as_index=False)
        .agg(article_count=("url", "count"))
        .sort_values("date")
        .reset_index(drop=True)
    )

    result["computed_at"] = datetime.now().isoformat()

    logger.info(f"[Gold] KPI 3 : {len(result)} jours calculés.")
    return result


#KPI 4: TOP CATÉGORIES PAR SOURCE

def compute_top_categories_by_source(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    -KPI 4: Top N catégories pour chaque source.
    -Permet de comprendre quels sujets dominent par source (bar chart groupé).

    -Colonnes résultantes :
        source        (str)  : nom de la source
        category      (str)  : nom de la catégorie
        article_count (int)  : nombre d'articles dans cette catégorie pour cette source
        rank          (int)  : rang (1 = catégorie la plus représentée)
    """
    logger.info(f"[Gold] Calcul KPI 4 : top {top_n} catégories par source...")

    if "category" not in df.columns:
        raise ValueError("[Gold] Colonne 'category' absente — vérifier la couche Silver.")

    #Nettoyer les catégories vides ou nulles
    df["category"] = df["category"].fillna("General").str.strip()
    df["category"] = df["category"].replace("", "General")

    #Compter les articles par (source, catégorie)
    grouped = (
        df.groupby(["source", "category"], as_index=False)
        .agg(article_count=("url", "count"))
    )

    #Classer par source et garder le top N
    grouped["rank"] = (
        grouped.groupby("source")["article_count"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    result = (
        grouped[grouped["rank"] <= top_n]
        .sort_values(["source", "rank"])
        .reset_index(drop=True)
    )

    result["computed_at"] = datetime.now().isoformat()

    logger.info(
        f"[Gold] KPI 4 : {len(result)} lignes (top {top_n} catégories × sources)."
    )
    return result


#KPI 5: TOP SOURCES PAR LANGUE

def compute_top_sources_by_language(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """
    -KPI 5: Top N sources les plus actives pour chaque langue détectée.
    -Permet de comprendre quelles sources produisent le plus de contenu
      dans chaque langue (utile pour Superset).

    -Colonnes résultantes :
        language      (str)  : code langue ISO
        source        (str)  : nom de la source
        article_count (int)  : nombre d'articles
        rank          (int)  : rang dans la langue (1 = source dominante)
    """
    logger.info(f"[Gold] Calcul KPI 5 : top {top_n} sources par langue...")

    if "language" not in df.columns:
        raise ValueError("[Gold] Colonne 'language' absente — vérifier la couche Silver.")

    df["language"] = df["language"].fillna("unknown")

    grouped = (
        df.groupby(["language", "source"], as_index=False)
        .agg(article_count=("url", "count"))
    )

    #Classer par langue et garder le top N
    grouped["rank"] = (
        grouped.groupby("language")["article_count"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    result = (
        grouped[grouped["rank"] <= top_n]
        .sort_values(["language", "rank"])
        .reset_index(drop=True)
    )

    result["computed_at"] = datetime.now().isoformat()

    logger.info(
        f"[Gold] KPI 5 : {len(result)} lignes (top {top_n} sources × langues)."
    )
    return result


#FONCTION PRINCIPALE

def process_silver_to_gold():
    """
    -Fonction principale appelée par le DAG 03_gold_analytics.
      Orchestre le chargement des données Silver et la création
      des 5 tables Gold dans MinIO.
    """
    logger.info("=" * 60)
    logger.info("[Gold] Démarrage de la transformation Silver -> Gold...")
    logger.info("=" * 60)

    #Étape 1: Charger toutes les données Silver
    df = load_all_silver_data()

    if df.empty:
        raise ValueError("[Gold] DataFrame Silver vide — arrêt de la transformation.")

    logger.info(f"[Gold] Données chargées : {len(df)} articles au total.")

    #Étape 2: Calculer les 5 KPIs
    kpis = {
        "articles_by_source/articles_by_source.parquet":               compute_articles_by_source(df),
        "articles_by_language/articles_by_language.parquet":           compute_articles_by_language(df),
        "articles_by_day/articles_by_day.parquet":                     compute_articles_by_day(df),
        "top_categories_by_source/top_categories_by_source.parquet":   compute_top_categories_by_source(df),
        "top_sources_by_language/top_sources_by_language.parquet":     compute_top_sources_by_language(df),
    }

    #Étape 3: Sauvegarder chaque table dans le bucket Gold
    saved = 0
    for object_name, df_kpi in kpis.items():
        try:
            path = save_parquet_to_layer(df_kpi, layer="gold", object_name=object_name)
            logger.info(f"[Gold] V Sauvegardé → {path} ({len(df_kpi)} lignes)")
            saved += 1
        except Exception as e:
            logger.error(f"[Gold] X Erreur sauvegarde '{object_name}' : {e}")

    logger.info("=" * 60)
    logger.info(f"[Gold] Transformation terminée : {saved}/5 tables créées.")
    logger.info("=" * 60)


#TEST LOCAL

if __name__ == "__main__":
    process_silver_to_gold()
