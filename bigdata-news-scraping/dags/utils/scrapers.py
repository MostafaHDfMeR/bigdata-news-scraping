'''
#Sources : CNN, BBC News, Hesport, Al Jazeera, NBC News, Reuters, Morocco World News
#Couche  : Bronze (données brutes)
'''

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import re
import time

logger = logging.getLogger(__name__)

# CLASSE DE BASE

class BaseScraper:
    """
    Classe parent partagée par tous les scrapers.
    Contient les méthodes communes : fetch_page, build_article, scrape_articles.
    """

    def __init__(self, source_name: str, base_url: str):
        self.source_name = source_name
        self.base_url = base_url
        # Simuler un vrai navigateur Chrome pour éviter d'être bloqué
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
        """
        Télécharger une page web avec gestion complète des erreurs.
        Retourne le HTML sous forme de texte, ou None si échec.
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=20)
            response.encoding = 'utf-8'
            response.raise_for_status()  # Erreur si status 4xx ou 5xx
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
            logger.error(f"[{self.source_name}] ERREUR INATTENDUE : {e}")
            return None

    def build_article(self, title: str, url: str, category: str = "General",
                      author: str = "", content: str = "", date_str: str = "") -> dict:
        """
        Construire un objet article standardisé.
        Toutes les sources produisent le même format → facile à traiter en Silver.
        """
        return {
            'title':            title.strip() if title else "Sans titre",
            'url':              url.strip() if url else "",
            'author':           author.strip() if author else "Unknown",
            'date_publication': date_str if date_str else datetime.now().isoformat(),
            'category':         category.strip() if category else "General",
            'content':          content.strip() if content else "",
            'source':           self.source_name,
            'scraped_at':       datetime.now().isoformat(),
            'language':         '',  # Sera détecté automatiquement en couche Silver
        }

    def scrape_articles(self) -> list[dict]:
        """
        Méthode obligatoire à implémenter dans chaque sous-classe.
        Si oubliée → erreur claire au lieu d'un comportement inattendu.
        """
        raise NotImplementedError(
            f"[{self.source_name}] La méthode scrape_articles() doit être implémentée."
        )

# CNN

class CNNScraper(BaseScraper):
   
    def __init__(self):
        super().__init__('cnn', 'https://www.cnn.com')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[CNN] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[CNN] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # CNN utilise des containers avec data-component-name
        cards = soup.find_all('div', attrs={'data-component-name': re.compile('card')})

        # Fallback 1 : chercher les liens avec classe contenant 'container'
        if not cards:
            cards = soup.find_all('div', class_=re.compile(r'container__item|card'))

        # Fallback 2 : balises article génériques
        if not cards:
            cards = soup.find_all('article')

        logger.info(f"[CNN] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre
                title_tag = (
                    card.find('span', class_=re.compile(r'container__headline|card__headline')) or
                    card.find('h2') or
                    card.find('h3') or
                    card.find('a')
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 10:
                    continue

                # URL
                link_tag = card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = 'https://www.cnn.com' + link

                # Catégorie
                cat_tag = card.find('span', class_=re.compile(r'eyebrow|category|label'))
                category = cat_tag.get_text(strip=True) if cat_tag else "News"

                article = self.build_article(title, link, category)
                articles.append(article)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"[CNN] Erreur parsing : {e}")
                continue

        logger.info(f"[CNN] {len(articles)} articles collectés.")
        return articles

# BBC NEWS

class BBCScraper(BaseScraper):
    
    def __init__(self):
        super().__init__('bbc_news', 'https://www.bbc.com/news')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[BBC News] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[BBC News] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # BBC utilise des data-testid pour identifier les cards
        cards = soup.find_all('div', attrs={'data-testid': re.compile(r'card|article')})

        # Fallback 1 : chercher les liens d'articles
        if not cards:
            cards = soup.find_all('li', attrs={'data-testid': re.compile(r'card')})

        # Fallback 2 : balises article
        if not cards:
            cards = soup.find_all('article')

        logger.info(f"[BBC News] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre
                title_tag = (
                    card.find('h3', attrs={'data-testid': 'card-headline'}) or
                    card.find('h2') or
                    card.find('h3') or
                    card.find('h4')
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 10:
                    continue

                # URL
                link_tag = card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = 'https://www.bbc.com' + link

                # Description courte
                desc_tag = card.find('p', attrs={'data-testid': 'card-description'})
                content = desc_tag.get_text(strip=True) if desc_tag else ""

                # Date
                time_tag = card.find('time')
                date_str = time_tag.get('datetime', '') if time_tag else ""

                article = self.build_article(title, link, "News", content=content, date_str=date_str)
                articles.append(article)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"[BBC News] Erreur parsing : {e}")
                continue

        logger.info(f"[BBC News] {len(articles)} articles collectés.")
        return articles

# SCRAPER 3 : HESPORT

class HesportScraper(BaseScraper):

    def __init__(self):
        super().__init__('hesport', 'https://www.hesport.com')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[Hesport] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[Hesport] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # Hesport utilise des cards avec classe 'post' ou 'article'
        cards = soup.find_all('div', class_=re.compile(r'post|article|card|item'))

        # Fallback : balises article
        if not cards:
            cards = soup.find_all('article')

        # Fallback 2 : listes d'articles
        if not cards:
            cards = soup.find_all('li', class_=re.compile(r'post|item|article'))

        logger.info(f"[Hesport] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre
                title_tag = (
                    card.find('h2') or
                    card.find('h3') or
                    card.find('h4') or
                    card.find('a', class_=re.compile(r'title|heading'))
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 5:
                    continue

                # URL
                link_tag = card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = self.base_url + link

                # Catégorie sportive
                cat_tag = card.find('span', class_=re.compile(r'cat|category|tag'))
                category = cat_tag.get_text(strip=True) if cat_tag else "Sport"

                # Date
                date_tag = card.find('time') or card.find('span', class_=re.compile(r'date|time'))
                date_str = ""
                if date_tag:
                    date_str = date_tag.get('datetime', '') or date_tag.get_text(strip=True)

                article = self.build_article(title, link, category, date_str=date_str)
                articles.append(article)
                time.sleep(0.2)

            except Exception as e:
                logger.warning(f"[Hesport] Erreur parsing : {e}")
                continue

        logger.info(f"[Hesport] {len(articles)} articles collectés.")
        return articles

# SCRAPER 4 : AL JAZEERA

class AlJazeeraScraper(BaseScraper):

    def __init__(self):
        super().__init__('aljazeera', 'https://www.aljazeera.com')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[Al Jazeera] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[Al Jazeera] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # Al Jazeera utilise des classes spécifiques pour ses articles
        cards = soup.find_all('article', class_=re.compile(r'article-card|teaser'))

        # Fallback 1 : div avec classe article
        if not cards:
            cards = soup.find_all('div', class_=re.compile(r'article-card|featured-articles'))

        # Fallback 2 : balises article génériques
        if not cards:
            cards = soup.find_all('article')

        logger.info(f"[Al Jazeera] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre
                title_tag = (
                    card.find('h3', class_=re.compile(r'article-card__title|heading')) or
                    card.find('h2') or
                    card.find('h3') or
                    card.find('h4')
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 10:
                    continue

                # URL
                link_tag = card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = 'https://www.aljazeera.com' + link

                # Catégorie
                cat_tag = card.find('span', class_=re.compile(r'article-card__category|section'))
                category = cat_tag.get_text(strip=True) if cat_tag else "World News"

                # Date
                time_tag = card.find('time')
                date_str = time_tag.get('datetime', '') if time_tag else ""

                # Description
                desc_tag = card.find('p', class_=re.compile(r'article-card__summary|description'))
                content = desc_tag.get_text(strip=True) if desc_tag else ""

                article = self.build_article(title, link, category, content=content, date_str=date_str)
                articles.append(article)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"[Al Jazeera] Erreur parsing : {e}")
                continue

        logger.info(f"[Al Jazeera] {len(articles)} articles collectés.")
        return articles

# NBC NEWS

class NBCNewsScraper(BaseScraper):

    def __init__(self):
        super().__init__('nbc_news', 'https://www.nbcnews.com')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[NBC News] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[NBC News] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # NBC News utilise des divs avec classe 'wide-card' ou 'tease-card'
        cards = soup.find_all('div', class_=re.compile(r'wide-card|tease-card|story-card'))

        # Fallback 1 : balises article
        if not cards:
            cards = soup.find_all('article')

        # Fallback 2 : sections avec titre
        if not cards:
            cards = soup.find_all('div', class_=re.compile(r'card|story|article'))

        logger.info(f"[NBC News] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre
                title_tag = (
                    card.find('h2', class_=re.compile(r'title|headline')) or
                    card.find('h3', class_=re.compile(r'title|headline')) or
                    card.find('h2') or
                    card.find('h3')
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 10:
                    continue

                # URL
                link_tag = card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = 'https://www.nbcnews.com' + link

                # Catégorie
                cat_tag = card.find('span', class_=re.compile(r'unibrow|category|label'))
                category = cat_tag.get_text(strip=True) if cat_tag else "News"

                # Description
                desc_tag = card.find('p', class_=re.compile(r'description|summary|dek'))
                content = desc_tag.get_text(strip=True) if desc_tag else ""

                # Auteur
                author_tag = card.find('span', class_=re.compile(r'byline|author'))
                author = author_tag.get_text(strip=True) if author_tag else ""

                article = self.build_article(title, link, category, author=author, content=content)
                articles.append(article)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"[NBC News] Erreur parsing : {e}")
                continue

        logger.info(f"[NBC News] {len(articles)} articles collectés.")
        return articles

# SCRAPER 6 : REUTERS

class ReutersScraper(BaseScraper):

    def __init__(self):
        super().__init__('reuters', 'https://www.reuters.com')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[Reuters] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[Reuters] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # Reuters utilise des attributs data-testid
        cards = soup.find_all('li', attrs={'data-testid': re.compile(r'story-item|article')})

        # Fallback 1 : divs avec attributs data
        if not cards:
            cards = soup.find_all('div', attrs={'data-testid': re.compile(r'story|article|card')})

        # Fallback 2 : balises article
        if not cards:
            cards = soup.find_all('article')

        # Fallback 3 : divs avec classe media-story
        if not cards:
            cards = soup.find_all('div', class_=re.compile(r'story|article|media'))

        logger.info(f"[Reuters] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre
                title_tag = (
                    card.find('a', attrs={'data-testid': 'Heading'}) or
                    card.find('h3', attrs={'data-testid': re.compile(r'heading|title')}) or
                    card.find('h2') or
                    card.find('h3')
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 10:
                    continue

                # URL
                link_tag = card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = 'https://www.reuters.com' + link

                # Catégorie
                cat_tag = card.find('span', attrs={'data-testid': re.compile(r'category|label')})
                if not cat_tag:
                    cat_tag = card.find('a', class_=re.compile(r'category|section'))
                category = cat_tag.get_text(strip=True) if cat_tag else "World"

                # Date
                time_tag = card.find('time')
                date_str = time_tag.get('datetime', '') if time_tag else ""

                # Description
                desc_tag = card.find('p', attrs={'data-testid': re.compile(r'body|description')})
                content = desc_tag.get_text(strip=True) if desc_tag else ""

                article = self.build_article(title, link, category, content=content, date_str=date_str)
                articles.append(article)
                time.sleep(0.3)

            except Exception as e:
                logger.warning(f"[Reuters] Erreur parsing : {e}")
                continue

        logger.info(f"[Reuters] {len(articles)} articles collectés.")
        return articles

# SCRAPER 7 : MOROCCO WORLD NEWS

class MoroccoWorldNewsScraper(BaseScraper):

    def __init__(self):
        super().__init__('morocco_world_news', 'https://www.moroccoworldnews.com')

    def scrape_articles(self) -> list[dict]:
        logger.info(f"[Morocco World News] Début du scraping...")
        html = self.fetch_page(self.base_url)
        if not html:
            logger.warning("[Morocco World News] Page principale inaccessible.")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # Morocco World News utilise WordPress — structure classique
        cards = soup.find_all('article', class_=re.compile(r'post|article|entry'))

        # Fallback 1 : divs avec classe post
        if not cards:
            cards = soup.find_all('div', class_=re.compile(r'post|article|card|entry'))

        # Fallback 2 : balises article génériques
        if not cards:
            cards = soup.find_all('article')

        logger.info(f"[Morocco World News] {len(cards)} éléments trouvés.")

        for card in cards[:15]:
            try:
                # Titre — WordPress utilise entry-title
                title_tag = (
                    card.find('h2', class_=re.compile(r'entry-title|post-title')) or
                    card.find('h3', class_=re.compile(r'entry-title|post-title')) or
                    card.find('h2') or
                    card.find('h3')
                )
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if len(title) < 10:
                    continue

                # URL
                link_tag = title_tag.find('a', href=True) or card.find('a', href=True)
                link = link_tag['href'] if link_tag else ""
                if link and not link.startswith('http'):
                    link = self.base_url + link

                # Catégorie — WordPress utilise cat-links
                cat_tag = card.find('span', class_=re.compile(r'cat|category|section'))
                if not cat_tag:
                    cat_tag = card.find('a', rel='category tag')
                category = cat_tag.get_text(strip=True) if cat_tag else "Morocco"

                # Date — WordPress utilise entry-date
                date_tag = card.find('time', class_=re.compile(r'entry-date|published'))
                date_str = ""
                if date_tag:
                    date_str = date_tag.get('datetime', '') or date_tag.get_text(strip=True)

                # Auteur — WordPress utilise author vcard
                author_tag = card.find('span', class_=re.compile(r'author|byline'))
                author = author_tag.get_text(strip=True) if author_tag else ""

                # Description
                desc_tag = card.find('div', class_=re.compile(r'entry-summary|excerpt'))
                content = desc_tag.get_text(strip=True) if desc_tag else ""

                article = self.build_article(
                    title, link, category,
                    author=author, content=content, date_str=date_str
                )
                articles.append(article)
                time.sleep(0.2)

            except Exception as e:
                logger.warning(f"[Morocco World News] Erreur parsing : {e}")
                continue

        logger.info(f"[Morocco World News] {len(articles)} articles collectés.")
        return articles

# FONCTION UTILITAIRE

def get_all_scrapers() -> list:

    return [
        CNNScraper(),
        BBCScraper(),
        HesportScraper(),
        AlJazeeraScraper(),
        NBCNewsScraper(),
        ReutersScraper(),
        MoroccoWorldNewsScraper(),
    ]