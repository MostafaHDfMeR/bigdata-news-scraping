"""
scrapers.py — Scripts de scraping pour le projet "bigdata-news-scraping"
Sources RSS  : The Guardian, Le Monde, RFI Francais, Hespress RSS
Sources HTML : BBC News, Al Jazeera, Euronews
Couche       : Bronze (données brutes)
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import re
import time

logger = logging.getLogger(__name__)


# CLASSE DE BASE

class BaseScraper:
    """Classe parent partagée par tous les scrapers."""

    def __init__(self, source_name: str, base_url: str):
        self.source_name = source_name
        self.base_url = base_url
        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8,ar;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Connection': 'keep-alive',
        }

    def fetch_page(self, url: str) -> str | None:
        """Télécharger une page web ou un flux RSS."""
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'
            response.raise_for_status()
            return response.text
        except requests.exceptions.Timeout:
            logger.error(f"[{self.source_name}] TIMEOUT : {url}")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"[{self.source_name}] CONNEXION REFUSÉE : {url}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"[{self.source_name}] ERREUR HTTP {e} : {url}")
            return None
        except Exception as e:
            logger.error(f"[{self.source_name}] ERREUR : {e}")
            return None

    def build_article(self, title: str, url: str, category: str = "General",
                      author: str = "", content: str = "", date_str: str = "") -> dict:
        """Construire un objet article standardisé."""
        return {
            'title':            title.strip() if title else "Sans titre",
            'url':              url.strip() if url else "",
            'author':           author.strip() if author else "Unknown",
            'date_publication': date_str if date_str else datetime.now().isoformat(),
            'category':         category.strip() if category else "General",
            'content':          content.strip() if content else "",
            'source':           self.source_name,
            'scraped_at':       datetime.now().isoformat(),
            'language':         '',
        }

    def scrape_articles(self) -> list[dict]:
        raise NotImplementedError(
            f"[{self.source_name}] scrape_articles() doit être implémentée."
        )


# CLASSE DE BASE RSS
# Partagée par tous les scrapers RSS (Guardian, France24, Arab News)

class BaseRSSScraper(BaseScraper):
    """
    Classe parent pour les scrapers basés sur les flux RSS.

    STRUCTURE D'UN FLUX RSS :
    <rss>
      <channel>
        <item>
          <title>Titre de l'article</title>
          <link>https://...</link>
          <description>Résumé...</description>
          <pubDate>Thu, 24 Apr 2026 14:00:00 GMT</pubDate>
          <author>Nom auteur</author>
          <category>Catégorie</category>
        </item>
      </channel>
    </rss>
    """

    def __init__(self, source_name: str, rss_url: str):
        super().__init__(source_name, rss_url)
        # Headers adaptés pour les flux RSS
        self.headers['Accept'] = 'application/rss+xml, application/xml, text/xml, */*'

    def parse_rss(self, xml_content: str) -> list[dict]:
        """
        Parser un flux RSS et retourner une liste d'articles.
        Fonctionne avec tous les flux RSS standard.
        """
        articles = []

        try:
            # Parser le XML avec BeautifulSoup
            # 'xml' parser ou 'lxml-xml' pour les flux RSS
            soup = BeautifulSoup(xml_content, 'xml')

            # Si 'xml' parser non disponible, on utilisent 'html.parser'
            if not soup.find('item'):
                soup = BeautifulSoup(xml_content, 'html.parser')

            # Trouver tous les éléments <item> du flux RSS
            items = soup.find_all('item')

            if not items:
                logger.warning(f"[{self.source_name}] Aucun item trouvé dans le RSS.")
                return []

            logger.info(f"[{self.source_name}] {len(items)} items RSS trouvés.")

            for item in items[:10]:
                try:
                    # Titre
                    title_tag = item.find('title')
                    title = title_tag.get_text(strip=True) if title_tag else ""
                    # Nettoyage des entités HTML dans le titre
                    title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title)
                    if len(title) < 5:
                        continue

                    # URL
                    link_tag = item.find('link')
                    if not link_tag or not link_tag.get_text(strip=True):
                        # Certains RSS utilisent <guid> pour l'URL
                        link_tag = item.find('guid')
                    link = link_tag.get_text(strip=True) if link_tag else ""

                    # Description / Contenu
                    # Recherche ultra-souple pour trouver le texte de l'article
                    content_tags = ['content:encoded', 'content', 'description', 'summary', 'encoded']
                    desc_tag = None
                    for tag_name in content_tags:
                        desc_tag = item.find(tag_name)
                        if desc_tag and len(desc_tag.get_text(strip=True)) > 10:
                            break
                    
                    content = ""
                    if desc_tag:
                        # Extraire le texte et gérer les blocs CDATA
                        raw = desc_tag.get_text(separator=' ', strip=True)
                        logger.info(f"[{self.source_name}] Balise trouvée : {desc_tag.name} | Texte : {raw[:50]}...")
                        
                        # Nettoyage
                        content = re.sub(r'<[^>]+>', '', raw)
                        try:
                            # Décodage des entités HTML (&amp; etc)
                            content = BeautifulSoup(content, "html.parser").get_text()
                        except:
                            pass
                        content = re.sub(r'\s+', ' ', content).strip()
                        content = content[:1000]

                    # Date
                    date_tag = (
                        item.find('pubDate') or
                        item.find('published') or
                        item.find('dc:date')
                    )
                    date_str = date_tag.get_text(strip=True) if date_tag else ""

                    # Auteur
                    # Recherche élargie pour l'auteur (gestion des namespaces dc:creator)
                    author_tags = ['dc:creator', 'creator', 'author', 'dc:author', 'byline']
                    author = "Unknown"
                    for a_tag in author_tags:
                        found_author = item.find(a_tag)
                        if found_author:
                            author = found_author.get_text(strip=True)
                            break
                    
                    # Nettoyage final de l'auteur (supprimer balises HTML si présentes)
                    author = re.sub(r'<[^>]+>', '', author).strip()
                    if not author:
                        author = "Unknown"

                    # Catégorie
                    cat_tag = item.find('category')
                    category = cat_tag.get_text(strip=True) if cat_tag else "General"
                    category = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', category)

                    article = self.build_article(
                        title, link, category,
                        author=author, content=content, date_str=date_str
                    )
                    articles.append(article)

                except Exception as e:
                    logger.warning(f"[{self.source_name}] Erreur parsing item : {e}")
                    continue

        except Exception as e:
            logger.error(f"[{self.source_name}] Erreur parsing RSS : {e}")

        return articles

    def scrape_articles(self) -> list[dict]:
        """Télécharger et parser le flux RSS."""
        logger.info(f"[{self.source_name}] Scraping RSS : {self.base_url}")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            logger.warning(f"[{self.source_name}] Flux RSS inaccessible.")
            return []

        articles = self.parse_rss(xml_content)
        logger.info(f"[{self.source_name}] {len(articles)} articles collectés via RSS.")
        return articles


# SCRAPER 1 : THE GUARDIAN (RSS)

class TheGuardianScraper(BaseRSSScraper):
    """
    Scraper pour The Guardian — actualités internationales en anglais.
    URL RSS : https://www.theguardian.com/world/rss
    Langue  : Anglais
    Pourquoi : RSS fiable, pas de JavaScript, contenu riche
    """

    def __init__(self):
        super().__init__(
            'the_guardian',
            'https://www.theguardian.com/world/rss'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[The Guardian] Scraping RSS en cours...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            # Fallback : essayer la section News
            logger.warning("[The Guardian] Flux World inaccessible, essai News...")
            xml_content = self.fetch_page('https://www.theguardian.com/news/rss')
            if not xml_content:
                return []

        articles = self.parse_rss(xml_content)
        logger.info(f"[The Guardian] {len(articles)} articles collectés.")
        return articles


# SCRAPER 2 : BBC NEWS (RSS)
# Le méme source amélioré avec RSS

class BBCScraper(BaseRSSScraper):
    """
    Scraper pour BBC News — actualités internationales en anglais.
    URL RSS : http://feeds.bbci.co.uk/news/rss.xml
    Langue  : Anglais
    Pourquoi : RSS officiel BBC, très fiable
    """

    def __init__(self):
        super().__init__(
            'bbc_news',
            'http://feeds.bbci.co.uk/news/rss.xml'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[BBC News] Scraping RSS en cours...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            # Fallback : essayer l'URL alternative
            logger.warning("[BBC News] RSS principal inaccessible, essai alternatif...")
            xml_content = self.fetch_page('https://feeds.bbci.co.uk/news/world/rss.xml')
            if not xml_content:
                return []

        articles = self.parse_rss(xml_content)
        logger.info(f"[BBC News] {len(articles)} articles collectés.")
        return articles


# SCRAPER 3 : LE MONDE (RSS)

class LeMondeScraper(BaseRSSScraper):
    """
    Le Monde — actualités françaises et internationales.
    URL RSS : https://www.lemonde.fr/rss/une.xml
    Langue  : Français
    """
    def __init__(self):
        super().__init__(
            'le_monde',
            'https://www.lemonde.fr/rss/une.xml'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[Le Monde] Scraping RSS...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            return []
        articles = self.parse_rss(xml_content)
        logger.info(f"[Le Monde] {len(articles)} articles collectés.")
        return articles


# SCRAPER 4 : AL JAZEERA (RSS)
# Le méme source amélioré avec RSS

class AlJazeeraScraper(BaseRSSScraper):
    """
    Scraper pour Al Jazeera English — actualités internationales en anglais.
    URL RSS : https://www.aljazeera.com/xml/rss/all.xml
    Langue  : Anglais
    Pourquoi : RSS officiel Al Jazeera, très fiable
    """

    def __init__(self):
        super().__init__(
            'aljazeera',
            'https://www.aljazeera.com/xml/rss/all.xml'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[Al Jazeera] Scraping RSS en cours...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            logger.warning("[Al Jazeera] RSS inaccessible.")
            return []

        articles = self.parse_rss(xml_content)
        logger.info(f"[Al Jazeera] {len(articles)} articles collectés.")
        return articles


# SCRAPER 5 : RFI FRANCAIS (RSS)

class RFIScraper(BaseRSSScraper):
    """
    RFI — Radio France Internationale.
    URL RSS : https://www.rfi.fr/fr/rss
    Langue  : Français
    """
    def __init__(self):
        super().__init__(
            'rfi',
            'https://www.rfi.fr/fr/rss'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[RFI] Scraping RSS...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            # Fallback
            xml_content = self.fetch_page(
                'https://www.rfi.fr/fr/podcasts/journal-en-francais-facile/rss'
            )
            if not xml_content:
                return []
        articles = self.parse_rss(xml_content)
        logger.info(f"[RFI] {len(articles)} articles collectés.")
        return articles


# SCRAPER 6 : HESPRESS (RSS)

class HespressRSSScraper(BaseRSSScraper):
    """
    Hespress — flux RSS officiel.
    URL RSS : https://fr.hespress.com/feed/
    Langue  : Français + Arabe
    """
    def __init__(self):
        super().__init__(
            'hespress',
            'https://fr.hespress.com/feed/'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[Hespress RSS] Scraping RSS...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            # Fallback : version arabe
            xml_content = self.fetch_page('https://hespress.com/feed/')
            if not xml_content:
                return []
        articles = self.parse_rss(xml_content)
        logger.info(f"[Hespress] {len(articles)} articles collectés.")
        return articles


# SCRAPER 7 : EURONEWS (RSS)

class EuronewsScraper(BaseRSSScraper):
    """
    Euronews France — actualités européennes en français.
    URL RSS : https://feeds.feedburner.com/euronews/fr/home
    Langue  : Français
    """
    def __init__(self):
        super().__init__(
            'euronews',
            'https://feeds.feedburner.com/euronews/fr/home'
        )

    def scrape_articles(self) -> list[dict]:
        logger.info("[Euronews] Scraping RSS...")
        xml_content = self.fetch_page(self.base_url)
        if not xml_content:
            # Fallback
            xml_content = self.fetch_page(
                'https://www.euronews.com/rss'
            )
            if not xml_content:
                return []
        articles = self.parse_rss(xml_content)
        logger.info(f"[Euronews] {len(articles)} articles collectés.")
        return articles


# FONCTION UTILITAIRE

def get_all_scrapers() -> list:
    """
    Retourner la liste de tous les scrapers disponibles.

        TheGuardianScraper  -> the_guardian
        BBCScraper          -> bbc_news
        LeMondeScraper     -> le_monde (RSS)
        AlJazeeraScraper    -> aljazeera
        RFIScraper          -> rfi_francais (RSS)
        EuronewsScraper     -> euronews (RSS)
    (site marocain simple) :
        HespressRSSScraper     -> hespress (RSS)
    """
    return [
        TheGuardianScraper(),
        BBCScraper(),
        LeMondeScraper(),
        AlJazeeraScraper(),
        RFIScraper(),
        HespressRSSScraper(),
        EuronewsScraper(),
    ]