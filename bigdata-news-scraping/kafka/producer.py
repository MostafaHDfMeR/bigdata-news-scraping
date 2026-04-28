"""
Producteur Kafka : Ingestion Streaming
Sources  : CNN, BBC News, Hesport, Al Jazeera, NBC News, Reuters, Morocco World News
Couche   : Bronze (streaming en temps réel)
Usage    : python kafka/producer.py

Différence avec le scraping Batch (bronze_scraping.py) :
  Batch     → Airflow déclenche le scraping toutes les heures
  Streaming → Ce script tourne EN CONTINU et envoie chaque article comme un événement
"""

import json
import time
import logging
import sys
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Ajouter le dossier dags/ au path pour importer les scrapers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# CONFIGURATION

# Adresse du broker Kafka
# - Depuis mon PC local       → 'localhost:9092'
# - Depuis un conteneur Docker → 'kafka:29092'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Nom du topic Kafka où seront envoyés les articles
TOPIC_NAME = 'news_articles'

# Pause entre chaque article envoyé (en secondes)
# 0.5s = simuler l'arrivée progressive des articles
DELAY_BETWEEN_ARTICLES = 0.5

# Pause entre chaque cycle de scraping (en secondes)
# 300s = 5 minutes entre chaque scraping de toutes les sources
DELAY_BETWEEN_CYCLES = 300

# Sources à scraper en streaming
# Tu peux commenter certaines sources pour les désactiver
STREAMING_SOURCES = [
    'cnn',
    'bbc_news',
    'hesport',
    'aljazeera',
    'nbc_news',
    'reuters',
    'morocco_world_news',
]

# CONNEXION KAFKA

def create_producer() -> KafkaProducer:
    """
    Création d'un producteur Kafka avec sérialisation JSON automatique.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(
                v, ensure_ascii=False
            ).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=500,   # Attendre 500ms entre chaque retry
        )
        logger.info("[Kafka] Producteur connecté avec succès.")
        return producer

    except NoBrokersAvailable:
        logger.error(
            "[Kafka] Impossible de se connecter aux brokers. "
            "Vérifier que Kafka est démarré : docker ps | grep kafka"
        )
        raise

# GESTION DU TOPIC

def create_topic_if_needed():
    """
    Création du topic 'news_articles' s'il n'existe pas encore.
    """
    from kafka.admin import KafkaAdminClient, NewTopic

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
        )
        existing_topics = admin_client.list_topics()

        if TOPIC_NAME not in existing_topics:
            # 7 partitions = une par source pour parallélisme optimal
            topic = NewTopic(
                name=TOPIC_NAME,
                num_partitions=7,
                replication_factor=1
            )
            admin_client.create_topics([topic])
            logger.info(
                f"[Kafka] Topic '{TOPIC_NAME}' créé "
                f"(7 partitions, 1 réplication)."
            )
        else:
            logger.info(f"[Kafka] Topic '{TOPIC_NAME}' existe déjà.")

        admin_client.close()

    except Exception as e:
        logger.error(f"[Kafka] Erreur création topic : {e}")
        raise

# CHARGEMENT DES SCRAPERS

def get_scrapers_for_streaming() -> list:
    """
    Chargement des scrapers de les 7 sources pour le streaming.
    Retourne seulement les scrapers dont la source est dans STREAMING_SOURCES.
    """
    from utils.scrapers import (
        CNNScraper,
        BBCScraper,
        HesportScraper,
        AlJazeeraScraper,
        NBCNewsScraper,
        ReutersScraper,
        MoroccoWorldNewsScraper,
    )

    # Mapping source_name → classe scraper
    all_scrapers = {
        'cnn':                CNNScraper(),
        'bbc_news':           BBCScraper(),
        'hesport':            HesportScraper(),
        'aljazeera':          AlJazeeraScraper(),
        'nbc_news':           NBCNewsScraper(),
        'reuters':            ReutersScraper(),
        'morocco_world_news': MoroccoWorldNewsScraper(),
    }

    # Retourner seulement les sources activées dans STREAMING_SOURCES
    active_scrapers = [
        scraper for name, scraper in all_scrapers.items()
        if name in STREAMING_SOURCES
    ]

    logger.info(
        f"[Kafka] {len(active_scrapers)} scrapers chargés : "
        f"{[s.source_name for s in active_scrapers]}"
    )
    return active_scrapers

# ENVOI DES ARTICLES

def send_article(producer: KafkaProducer, article: dict) -> bool:
    """
    Envoyer un seul article vers le topic Kafka.
    La clé du message = "source_hashUrl"
    Exemple : "cnn_-4823947293847"
    Pourquoi une clé ?
    Kafka utilise la clé pour router le message vers la bonne partition.
    Tous les articles CNN vont dans la même partition → ordre garanti.
    """
    try:
        # Construction d'une clé unique pour l'article
        key = f"{article['source']}_{hash(article['url'])}"

        # Envoya du message et attendre la confirmation
        future = producer.send(TOPIC_NAME, key=key, value=article)
        future.get(timeout=10)  # Attendre max 10 secondes

        logger.debug(
            f"[Kafka] ✔ Envoyé [{article['source']}] : "
            f"{article['title'][:60]}..."
        )
        return True

    except Exception as e:
        logger.error(
            f"[Kafka] ✘ Erreur envoi article "
            f"[{article.get('source', '?')}] : {e}"
        )
        return False

# BOUCLE PRINCIPALE DE STREAMING

def stream_all_sources():
    """
    Boucle principale du streaming.
    Flux :
    CNN scraper      → articles CNN      → Kafka topic 'news_articles'
    BBC scraper      → articles BBC      → Kafka topic 'news_articles'
    Hesport scraper  → articles Hesport  → Kafka topic 'news_articles'
    etc....
    """
    # Initialisation
    create_topic_if_needed()
    producer = create_producer()
    scrapers = get_scrapers_for_streaming()

    logger.info("=" * 60)
    logger.info("[Kafka] Démarrage du streaming en continu...")
    logger.info(f"[Kafka] Topic : {TOPIC_NAME}")
    logger.info(f"[Kafka] Sources : {[s.source_name for s in scrapers]}")
    logger.info(f"[Kafka] Cycle : toutes les {DELAY_BETWEEN_CYCLES // 60} minutes")
    logger.info("=" * 60)

    cycle_number = 0

    # Boucle infinie — tourne jusqu'à Ctrl+C
    while True:
        cycle_number += 1
        cycle_start = datetime.now()

        logger.info(
            f"\n[Kafka] ══ CYCLE #{cycle_number} — "
            f"{cycle_start.strftime('%H:%M:%S')} ══"
        )

        total_sent = 0
        total_errors = 0

        # Scraper chaque source et envoyer les articles
        for scraper in scrapers:
            logger.info(f"[Kafka] Scraping de '{scraper.source_name}'...")

            try:
                articles = scraper.scrape_articles()

                if not articles:
                    logger.warning(
                        f"[Kafka] '{scraper.source_name}' : "
                        f"aucun article collecté."
                    )
                    continue

                logger.info(
                    f"[Kafka] '{scraper.source_name}' : "
                    f"{len(articles)} articles à envoyer."
                )

                # Envoyer chaque article individuellement
                for article in articles:
                    success = send_article(producer, article)
                    if success:
                        total_sent += 1
                    else:
                        total_errors += 1

                    # Pause entre chaque article pour simuler
                    # l'arrivée progressive en temps réel
                    time.sleep(DELAY_BETWEEN_ARTICLES)

            except Exception as e:
                logger.error(
                    f"[Kafka] Erreur scraping '{scraper.source_name}' : {e}"
                )
                continue

        # Vider le buffer — s'assurer que tous les messages sont envoyés
        producer.flush()

        # Résumé du cycle
        duration = (datetime.now() - cycle_start).seconds
        logger.info(
            f"[Kafka] Cycle #{cycle_number} terminé en {duration}s — "
            f"{total_sent} articles envoyés, "
            f"{total_errors} erreurs."
        )

        # Attendre avant le prochain cycle
        logger.info(
            f"[Kafka] Prochaine collecte dans "
            f"{DELAY_BETWEEN_CYCLES // 60} minutes..."
        )
        time.sleep(DELAY_BETWEEN_CYCLES)

# POINT D'ENTRÉE

if __name__ == '__main__':
    try:
        stream_all_sources()
    except KeyboardInterrupt:
        logger.info("\n[Kafka] Streaming arrêté par l'utilisateur (Ctrl+C).")
    except Exception as e:
        logger.error(f"[Kafka] Erreur fatale : {e}")
        raise