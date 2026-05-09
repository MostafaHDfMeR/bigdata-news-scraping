# 📰 Big Data News Scraping Pipeline

Ce projet implémente un pipeline de données Big Data complet pour le scrapage, le nettoyage et l'analyse d'actualités provenant de plusieurs sources internationales (The Guardian, BBC, Le Monde, Al Jazeera, RFI, Hespress, Euronews).

## 🏗️ Architecture du Projet (Médaillon)

Le projet suit l'architecture **Médaillon** pour garantir une gestion propre des données :

1.  **Couche BRONZE (Données Brutes)** :
    - Les scrapers extraient les articles via des flux RSS.
    - Les données sont stockées telles quelles au format **JSON** dans MinIO.
    - Dossier : `bronze/{source}/{année}/{mois}/{jour}/`

2.  **Couche SILVER (Données Nettoyées)** :
    - Traitement avec **Pandas**.
    - **Nettoyage HTML** : Suppression des balises avec BeautifulSoup.
    - **Normalisation** : Gestion des espaces et caractères spéciaux.
    - **Détection de Langue** : Identification automatique (en, fr, ar) basée sur le titre et le contenu.
    - Stockage au format **Parquet** (compressé et rapide).
    - Dossier : `silver/{source}/{année}/{mois}/{jour}/`

3.  **Couche GOLD (Analyse)** : *(En cours)*
    - Agrégations et statistiques prêtes pour Power BI.

## ⚙️ Orchestration avec Airflow

Nous avons structuré le dossier `dags/` pour répondre aux exigences d'Airflow :
- **`dags/01_bronze_scraping.py`** : Gère le scrapage toutes les heures. Nous avons sorti ce fichier à la racine du dossier `dags/` pour qu'Airflow puisse le détecter automatiquement comme un point d'entrée.
- **`dags/02_silver_cleaning.py`** : Gère le nettoyage des données. Il est séparé pour permettre de relancer le nettoyage indépendamment du scrapage.
- **`dags/utils/`** : Ce sous-dossier contient toute la logique technique (scripts de scrapage, client MinIO, fonctions de nettoyage). Cela permet de garder les fichiers DAG très courts et lisibles.

## 🚀 Installation et Démarrage

### Pré-requis
- Docker Desktop
- Git

### Lancement
```powershell
docker-compose up -d --build
```

### Accès aux outils
- **Airflow** : [http://localhost:8080](http://localhost:8080) (Login/Mdp: `airflow`)
- **MinIO (Data Lake)** : [http://localhost:9001](http://localhost:9001) (Login/Mdp: `minioadmin` / `minioadmin123`)
- **Kafdrop (Kafka UI)** : [http://localhost:9200](http://localhost:9200)

## 🛠️ Technologies utilisées
- **Python** (Pandas, BeautifulSoup, Langdetect)
- **Apache Airflow** (Orchestration)
- **MinIO** (Stockage S3-compatible)
- **Apache Kafka** (Streaming de données)
- **Docker** (Conteneurisation)
- **Parquet** (Format de fichier binaire)

---
*Projet développé par Mostafa et Chaymaa.*
