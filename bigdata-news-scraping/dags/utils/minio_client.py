"""
Sources  : CNN, BBC News, Hesport, Al Jazeera, NBC News, Reuters, Morocco World News
Couches  : Bronze (JSON), Silver (Parquet), Gold (Parquet)
"""

import os
import json
import logging
from io import BytesIO
from datetime import datetime
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

# CONFIGURATION — lire depuis les variables d'environnement Airflow (.env)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS   = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET   = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

# Les 3 buckets du Data Lake — correspondent aux 3 couches Médaillon
BUCKETS = ["bronze", "silver", "gold"]

# Les 7 sources valides — correspondent aux classes dans scrapers.py
VALID_SOURCES = [
    "cnn",
    "bbc_news",
    "hesport",
    "aljazeera",
    "nbc_news",
    "reuters",
    "morocco_world_news",
]

# CONNEXION

def get_minio_client() -> Minio:
    """
    Créer et retourner un client MinIO configuré.
    Utilisé par toutes les fonctions de ce fichier.
    """
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False  # False car on est en local (pas de HTTPS nécessaire)
    )

# INITIALISATION DES BUCKETS

def init_all_buckets():
    """
    Créer les 3 buckets Bronze, Silver, Gold s'ils n'existent pas encore.
    À appeler une seule fois au démarrage du projet.
    """
    client = get_minio_client()

    for bucket in BUCKETS:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"[MinIO] Bucket '{bucket}' créé.")
            else:
                logger.info(f"[MinIO] Bucket '{bucket}' existe déjà.")
        except S3Error as e:
            logger.error(f"[MinIO] Erreur création bucket '{bucket}' : {e}")

    logger.info("[MinIO] Initialisation des buckets terminée.")

# COUCHE BRONZE — Sauvegarde JSON

def save_json_to_bronze(data: list[dict], source_name: str) -> str:
    """
    Sauvegarder une liste d'articles JSON dans le bucket Bronze.
    """
    # Vérification que la source est valide
    if source_name not in VALID_SOURCES:
        logger.warning(
            f"[MinIO Bronze] Source inconnue '{source_name}'. "
            f"Sources valides : {VALID_SOURCES}"
        )

    client = get_minio_client()
    bucket = "bronze"

    # Créer le bucket si nécessaire
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info(f"[MinIO] Bucket '{bucket}' créé automatiquement.")

    # Construire le nom de l'objet avec organisation par date
    # Format : source_name/AAAA/MM/JJ/source_name_HHMMSS.json
    now = datetime.now()
    object_name = (
        f"{source_name}/"
        f"{now.strftime('%Y/%m/%d')}/"
        f"{source_name}_{now.strftime('%H%M%S')}.json"
    )

    # Convertir la liste d'articles en bytes JSON
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    data_stream = BytesIO(json_bytes)

    # Vers MinIO
    try:
        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=data_stream,
            length=len(json_bytes),
            content_type="application/json"
        )
        logger.info(
            f"[MinIO Bronze] {len(data)} articles de '{source_name}' "
            f"sauvegardés → s3://bronze/{object_name}"
        )
    except S3Error as e:
        logger.error(f"[MinIO Bronze] Erreur sauvegarde '{source_name}' : {e}")
        raise

    return f"s3://bronze/{object_name}"

# COUCHES SILVER et GOLD — Sauvegarde Parquet

def save_parquet_to_layer(df, layer: str, object_name: str) -> str:
    """
    Sauvegarder un DataFrame pandas en format Parquet dans Silver ou Gold.

    Utilisé par :
    - silver_cleaning.py  → layer="silver"
    - gold_analytics.py   → layer="gold"

    Retourne le chemin complet de l'objet créé.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Vérification que le layer est valide
    if layer not in ["silver", "gold"]:
        raise ValueError(f"[MinIO] Layer invalide '{layer}'. Utiliser 'silver' ou 'gold'.")

    client = get_minio_client()

    # Créer le bucket si nécessaire
    if not client.bucket_exists(layer):
        client.make_bucket(layer)
        logger.info(f"[MinIO] Bucket '{layer}' créé automatiquement.")

    # Convertir DataFrame → Parquet en mémoire (sans écrire sur disque)
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    parquet_bytes = buffer.read()

    # Vers MinIO
    try:
        client.put_object(
            bucket_name=layer,
            object_name=object_name,
            data=BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream"
        )
        logger.info(
            f"[MinIO {layer.upper()}] {len(df)} lignes "
            f"sauvegardées → s3://{layer}/{object_name}"
        )
    except S3Error as e:
        logger.error(f"[MinIO {layer.upper()}] Erreur sauvegarde : {e}")
        raise

    return f"s3://{layer}/{object_name}"

# LECTURE DES DONNÉES

def list_objects(bucket: str, prefix: str = "") -> list[str]:
    """
    Lister tous les objets dans un bucket.
    """
    client = get_minio_client()
    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        result = [obj.object_name for obj in objects]
        logger.info(f"[MinIO] {len(result)} objets trouvés dans '{bucket}/{prefix}'")
        return result
    except S3Error as e:
        logger.error(f"[MinIO] Erreur listing '{bucket}' : {e}")
        return []


def list_objects_by_source(source_name: str) -> list[str]:
    """
    Lister tous les fichiers Bronze d'une source spécifique.
    """
    if source_name not in VALID_SOURCES:
        logger.warning(f"[MinIO] Source inconnue : '{source_name}'")
        return []

    return list_objects("bronze", prefix=f"{source_name}/")


def get_json_from_bronze(object_name: str) -> list[dict]:
    """
    Télécharger et désérialiser un fichier JSON depuis le bucket Bronze.
    """
    client = get_minio_client()
    try:
        response = client.get_object("bronze", object_name)
        data = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        logger.info(f"[MinIO Bronze] Lu : {object_name} ({len(data)} articles)")
        return data
    except S3Error as e:
        logger.error(f"[MinIO Bronze] Erreur lecture '{object_name}' : {e}")
        return []


def get_parquet_from_layer(layer: str, object_name: str):
    """
    Télécharger un fichier Parquet depuis Silver ou Gold
    et retourner un DataFrame pandas.
    """
    import pandas as pd
    import pyarrow.parquet as pq

    client = get_minio_client()
    try:
        response = client.get_object(layer, object_name)
        buffer = BytesIO(response.read())
        response.close()
        response.release_conn()
        df = pd.read_parquet(buffer)
        logger.info(
            f"[MinIO {layer.upper()}] Lu : {object_name} ({len(df)} lignes)"
        )
        return df
    except S3Error as e:
        logger.error(f"[MinIO {layer.upper()}] Erreur lecture '{object_name}' : {e}")
        raise

# UTILITAIRES

def get_today_bronze_objects() -> list[str]:
    """
    Retourner tous les fichiers Bronze créés aujourd'hui.
    Utilisé par le DAG Silver pour savoir quoi traiter.
    """
    today = datetime.now().strftime('%Y/%m/%d')
    all_objects = list_objects("bronze")
    today_objects = [o for o in all_objects if today in o]
    logger.info(f"[MinIO Bronze] {len(today_objects)} fichiers trouvés pour aujourd'hui ({today})")
    return today_objects

def get_bronze_objects_by_source_today(source_name: str) -> list[str]:
    """
    Retourner les fichiers Bronze d'une source spécifique créés aujourd'hui.
    """
    today = datetime.now().strftime('%Y/%m/%d')
    source_objects = list_objects_by_source(source_name)
    return [o for o in source_objects if today in o]

def print_storage_summary():
    """
    Afficher un résumé du stockage dans les 3 buckets.
    Utile pour déboguer et vérifier l'état du Data Lake.
    """
    print("\n" + "="*50)
    print("   RÉSUMÉ DU DATA LAKE MINIO")
    print("="*50)

    for bucket in BUCKETS:
        objects = list_objects(bucket)
        print(f"\n  Bucket '{bucket}' : {len(objects)} fichiers")

        # Afficher par source pour Bronze
        if bucket == "bronze":
            for source in VALID_SOURCES:
                source_files = [o for o in objects if o.startswith(source + "/")]
                if source_files:
                    print(f"    └── {source:<25} : {len(source_files)} fichiers")

    print("\n" + "="*50 + "\n")