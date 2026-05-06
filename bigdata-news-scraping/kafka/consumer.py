"""
Consumer Kafka → MinIO Bronze
Lit les articles depuis le topic Kafka 'news_articles'
et les sauvegarde dans le bucket MinIO 'bronze'.
"""

import json
import logging
import os
import sys
from collections import defaultdict
from kafka import KafkaConsumer

# Permet d'importer dags/utils/minio_client.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from utils.minio_client import save_json_to_bronze

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
TOPIC_NAME = 'news_articles'
GROUP_ID = 'minio-bronze-consumer'

# Taille du lot avant écriture dans MinIO
FLUSH_SIZE = 10

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
)

logger.info("Consumer Kafka démarré")
logger.info(f"Topic écouté : {TOPIC_NAME}")
logger.info(f"Consumer group : {GROUP_ID}")
logger.info("Destination : MinIO bucket bronze")

buffer_by_source = defaultdict(list)

try:
    for message in consumer:
        article = message.value
        source = article.get('source', 'unknown')

        buffer_by_source[source].append(article)

        logger.info(
            f"[Kafka] Article reçu | source={source} | "
            f"title={article.get('title', '')[:70]}"
        )

        if len(buffer_by_source[source]) >= FLUSH_SIZE:
            logger.info(
                f"[MinIO] Écriture de {len(buffer_by_source[source])} "
                f"articles vers bronze pour source='{source}'"
            )
            save_json_to_bronze(buffer_by_source[source], source)
            buffer_by_source[source].clear()

except KeyboardInterrupt:
    logger.info("Arrêt demandé par l'utilisateur.")

    # Sauvegarder les articles restants dans les buffers avant arrêt
    for source, articles in buffer_by_source.items():
        if articles:
            logger.info(
                f"[MinIO] Flush final : {len(articles)} articles "
                f"pour source='{source}'"
            )
            save_json_to_bronze(articles, source)

finally:
    consumer.close()
    logger.info("Consumer Kafka arrêté proprement.")