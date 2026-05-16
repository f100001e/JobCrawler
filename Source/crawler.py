import os
import time
import json
import requests
import tldextract
import sqlite3
import csv
import random
import re


import yaml
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
import xml.etree.ElementTree as ET

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_PATH = (BASE_DIR / os.getenv("DB_PATH", "metacrawler.db")).resolve()
print("USING DB:", DB_PATH)
_dsf = os.getenv("DATABASE_SOURCES_FILE", "").strip()
DATABASE_SOURCES_FILE = (BASE_DIR / _dsf) if _dsf else None

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
USER_AGENT = "MetaCrawler/1.0 (+polite; research)"
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY_SECONDS", "2.0"))  # Be extra polite with free sources

MAX_COMPANIES_PER_DAY = int(os.getenv("MAX_COMPANIES_PER_DAY", "50"))
MAX_CONTACTS_PER_COMPANY = int(os.getenv("MAX_CONTACTS_PER_COMPANY", "10"))

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# Near HR_KEYWORDS and ENG_KEYWORDS, add:
DECISION_MAKER_KEYWORDS = (
"ceo", "cfo", "cto", "coo", "cmo", "chief", "president", "founder", "owner",
"director", "vp", "vice president", "head of", "manager", "lead", "executive",
"decision", "strategic", "business", "operations", "product", "sales", "marketing",
"revenue", "growth", "strategy"
)
HR_KEYWORDS = ("hr", "human resources", "recruiting", "talent", "people", "careers", "jobs", "hiring", "director")
ENG_KEYWORDS = ("engineering", "engineer", "eng", "dev", "developer")

GENERIC_PREFIXES = {"jobs", "info", "service", "support", "hello", "contact", "team", "admin", "noreply", "no-reply"}

def is_generic_email(email: str) -> bool:
    prefix = email.split("@")[0].lower().split("+")[0]
    return prefix in GENERIC_PREFIXES

# ===== FREE PUBLIC DATABASE SOURCES =====
def load_free_database_sources() -> Dict:
    """Load database sources with or without API keys.
    YAML is treated as overrides (enabled/path/params/etc) unless it defines a brand-new source.
    """

    defaults = {
    # ===== ALL DATASETS (WORKING) =====

    "apollo_people": {
        "name": "Apollo.io People Search",
        "url": "https://api.apollo.io/api/v1/mixed_people/api_search",
        "type": "apollo",
        "enabled": False,
        "parser": "apollo_people",
        "description": "Apollo.io verified contacts at funded companies",
        "estimated_companies": 500,
    },

    "github_startup_resources": {
        "name": "GitHub Startup Resources",
        "url": "https://raw.githubusercontent.com/mmccaff/PlacesToPostYourStartup/master/README.md",
        "type": "markdown",
        "enabled": False,
        "parser": "github_markdown",
        "description": "Places to post your startup",
        "estimated_companies": 200,
    },

    # ===== CNCF LANDSCAPE (real companies, real URLs) =====
    "cncf_landscape": {
        "name": "CNCF Landscape",
        "url": "https://raw.githubusercontent.com/cncf/landscape/refs/heads/master/landscape.yml",
        "type": "yaml_remote",
        "enabled": False,
        "parser": "cncf_landscape",
        "description": "Cloud Native Computing Foundation member companies",
        "estimated_companies": 800,
    },

    # ===== TECH NEWS/RSS =====
    "hacker_news_whoishiring": {
        "name": "Hacker News Who is Hiring",
        "url": "https://hn.algolia.com/api/v1/search?tags=story,author_whoishiring",
        "type": "json",
        "enabled": False,
        "parser": "hn_whoishiring",
        "description": "HN Who is Hiring posts",
        "estimated_companies": 1000,
    },

    # ===== LOCAL FILES =====
    "local_csv": {
        "name": "Local CSV File",
        "path": "companies.csv",
        "type": "local_csv",
        "enabled": False,
        "parser": "csv",
        "description": "Your own CSV with company data",
        "estimated_companies": "variable",
    },
    "yaml_companies": {
        "name": "YAML Companies File",
        "path": "companies.yml",
        "type": "local_yaml",
        "enabled": False,
        "parser": "yaml_companies",
        "description": "Curated list of target company endpoints",
        "estimated_companies": "variable",
    },

    # ===== DISABLED / OFTEN BLOCKED =====
    "techcrunch_feed": {
        "name": "TechCrunch RSS Feed",
        "url": "https://techcrunch.com/feed/",
        "type": "rss",
        "enabled": False,
        "parser": "rss_feed",
        "description": "Low yield (1 company), disabled",
        "estimated_companies": 1,
    },
    "edgar_companies": {
        "name": "SEC EDGAR Company List",
        "url": "https://www.sec.gov/files/company_tickers.json",
        "type": "json",
        "enabled": False,
        "parser": "edgar_companies",
        "description": "Disabled - fake domains, no real URLs",
        "estimated_companies": 0,
    },
    "angel_list_public": {
        "name": "AngelList Public Pages",
        "url": "https://angel.co/companies",
        "type": "html",
        "enabled": False,
        "parser": "angel_list_scrape",
        "description": "403 blocked",
        "estimated_companies": 0,
    },
    "product_hunt_public": {
        "name": "Product Hunt Today",
        "url": "https://www.producthunt.com/",
        "type": "html",
        "enabled": False,
        "parser": "product_hunt_scrape",
        "description": "JS-rendered, yields 0",
        "estimated_companies": 0,
    },
    "indie_hackers": {
        "name": "Indie Hackers Products",
        "url": "https://www.indiehackers.com/products",
        "type": "html",
        "enabled": False,
        "parser": "indie_hackers_scrape",
        "description": "JS-rendered, yields 0",
        "estimated_companies": 0,
    },
}

    # Apply YAML overrides
    if DATABASE_SOURCES_FILE and DATABASE_SOURCES_FILE.exists():
        try:
            with open(DATABASE_SOURCES_FILE, "r", encoding="utf-8") as f:
                overrides = yaml.safe_load(f) or {}

            if not isinstance(overrides, dict):
                print(f"Note: {DATABASE_SOURCES_FILE} must be a mapping of source_id -> config")
                return defaults

            for sid, ov in overrides.items():
                if sid in defaults and isinstance(ov, dict):
                    defaults[sid].update(ov)
                else:
                    defaults[sid] = ov  # allow brand-new sources
        except Exception as e:
            print(f"Note: Could not load {DATABASE_SOURCES_FILE}: {e}")

    return defaults

def parse_cncf_landscape(data: Any) -> List[Dict]:
    companies = []
    if not isinstance(data, str):
        return companies
    landscape = yaml.safe_load(data)
    if not landscape:
        return companies
    categories = landscape if isinstance(landscape, list) else landscape.get('landscape', [])
    for category in categories:
        for subcategory in category.get('subcategories') or []:
            for item in subcategory.get('items') or []:
                url = item.get('homepage_url') or item.get('homepageurl')
                name = item.get('name', '')
                if url and name:
                    companies.append({
                        'name': name,
                        'url': url,
                        'source': 'cncf_landscape',
                        'metadata': {'category': category.get('name', '')}
                    })
    return companies

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def extract_domain(url: str) -> str:
    """Extract clean domain from URL"""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        ext = tldextract.extract(url)
        if not ext.domain or not ext.suffix:
            raise ValueError(f"Could not extract domain from: {url}")
        return f"{ext.domain}.{ext.suffix}".lower()
    except Exception as e:
        raise ValueError(f"Error extracting domain from {url}: {e}")


# ===== PARSER FUNCTIONS =====

def parse_apollo_people(data: Any) -> List[Dict]:
    companies = []
    if not isinstance(data, dict):
        return companies

    for person in data.get('people', []) or []:
        org = person.get('organization') or {}
        email = person.get('email', '')
        url = org.get('website_url') or org.get('primary_domain') or ''

        if not email:
            continue

        if not url and '@' in email:
            url = 'https://' + email.split('@')[1]

        companies.append({
            'name': org.get('name', ''),
            'url': url if url.startswith('http') else f"https://{url}",
            'source': 'apollo_people',
            'metadata': {
                'contact_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                'contact_email': email,
                'contact_title': person.get('title', ''),
            }
        })
    return companies

def parse_yc_json(data: Any) -> List[Dict]:
    """Parse Y Combinator JSON export"""
    companies = []
    if isinstance(data, list):
        for company in data:
            if isinstance(company, dict) and company.get('website'):
                companies.append({
                    'name': company.get('name', ''),
                    'url': company['website'],
                    'source': 'yc_export',
                    'metadata': {
                        'batch': company.get('batch'),
                        'status': company.get('status', 'active')
                    }
                })
    return companies


def parse_edgar_companies(data: Any) -> List[Dict]:
    """Parse SEC EDGAR company data"""
    companies = []
    if isinstance(data, dict):
        for cik, info in data.items():
            if isinstance(info, dict):
                # EDGAR doesn't have website, but we can construct from company name
                name = info.get('title', '')
                if name:
                    # Try to create a plausible domain
                    clean_name = re.sub(r'[^\w\s]', '', name.lower())
                    base_name = clean_name.split()[0] if clean_name.split() else ''
                    if base_name:
                        companies.append({
                            'name': name,
                            'url': f"https://{base_name}.com",
                            'source': 'edgar',
                            'metadata': info
                        })
    return companies

def parse_opencorporates(data: Any) -> List[Dict]:
    """Parse OpenCorporates API response"""
    companies = []
    if isinstance(data, dict):
        results = data.get('results', {}).get('companies', [])
        for company in results:
            company_data = company.get('company', {})
            website = company_data.get('website_url')
            if website:
                companies.append({
                    'name': company_data.get('name', ''),
                    'url': website,
                    'source': 'opencorporates',
                    'metadata': company_data
                })
    return companies

def parse_hn_whoishiring(data: Any) -> List[Dict]:
    """Parse Hacker News Who is Hiring posts"""
    companies = []
    if isinstance(data, dict):
        hits = data.get('hits', [])
        for hit in hits:
            text = hit.get('title', '') + ' ' + hit.get('text', '')
            # Look for company names in the text
            # Simple regex to find potential company mentions
            company_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:is hiring|hiring)\b'
            matches = re.findall(company_pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match.split()) <= 4:  # Likely a company name
                    companies.append({
                        'name': match,
                        'url': f"https://{match.lower().replace(' ', '')}.com",
                        'source': 'hn_hiring',
                        'metadata': {'hn_id': hit.get('objectID')}
                    })
    return companies

def parse_sitemap_urls(xml_content: str) -> List[Dict]:
    """Parse sitemap XML for URLs"""
    companies = []
    try:
        root = ET.fromstring(xml_content)
        # Look for URLs in sitemap
        namespaces = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = root.findall('.//sm:loc', namespaces) or root.findall('.//loc')

        for url_elem in urls[:100]:  # Limit to first 100
            url = url_elem.text
            if url and ("company" in url.lower() or "business" in url.lower()):
                try:
                    domain = extract_domain(url)
                    companies.append({
                        'name': domain.split('.')[0].replace('-', ' ').title(),
                        'url': url,
                        'source': 'sitemap',
                        'metadata': {'url': url}
                    })
                except:
                    continue
    except Exception as e:
        print(f"Error parsing sitemap: {e}")

    return companies

def parse_rss_feed(xml_content: str) -> List[Dict]:
    """Parse RSS feed for company mentions"""
    companies = []
    try:
        root = ET.fromstring(xml_content)

        # Look for items/articles
        for item in root.findall('.//item')[:50]:  # Limit to 50 items
            title = item.find('title')
            link = item.find('link')
            description = item.find('description')

            if title is not None and title.text:
                # Extract potential company names from title
                text = title.text
                if description is not None and description.text:
                    text += ' ' + description.text

                # Look for patterns like "Company raises $", "Company launches"
                patterns = [
                    r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:raises|launches|announces|secures)\b',
                    r'\b(?:raised by|backed by|invested in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b'
                ]

                for pattern in patterns:
                    matches = re.findall(pattern, text)
                    for match in matches:
                        if 1 <= len(match.split()) <= 3:  # Reasonable company name length
                            companies.append({
                                'name': match,
                                'url': f"https://{match.lower().replace(' ', '')}.com",
                                'source': 'rss_feed',
                                'metadata': {'title': title.text[:100]}
                            })
                            break  # Only take first match per item
    except Exception as e:
        print(f"Error parsing RSS: {e}")

    return list({c['name']: c for c in companies}.values())  # Deduplicate by name

def parse_github_markdown(text: str) -> List[Dict]:
    """Extract company URLs from GitHub markdown"""
    companies = []

    # Look for markdown links
    url_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    matches = re.findall(url_pattern, text)

    for link_text, url in matches:
        if url.startswith('http'):
            try:
                domain = extract_domain(url)
                # Skip common non-company domains
                skip_domains = ['github.com', 'twitter.com', 'linkedin.com', 'youtube.com',
                                'medium.com', 'wikipedia.org', 'google.com', 'producthunt.com']

                if not any(skip in domain for skip in skip_domains):
                    companies.append({
                        'name': link_text[:50],
                        'url': url,
                        'source': 'github_md',
                        'metadata': {'link_text': link_text}
                    })
            except:
                continue

    # Also look for bare URLs
    bare_url_pattern = r'https?://[^\s\)\]>]+'
    bare_matches = re.findall(bare_url_pattern, text)

    for url in bare_matches:
        if url not in [c['url'] for c in companies]:  # Avoid duplicates
            try:
                domain = extract_domain(url)
                if '.' in domain and len(domain) > 4:
                    companies.append({
                        'name': domain.split('.')[0].replace('-', ' ').title(),
                        'url': url,
                        'source': 'github_md',
                        'metadata': {'url': url}
                    })
            except:
                continue

    return companies

def parse_plain_text(text: str) -> List[Dict]:
    """Parse plain text domains"""
    companies = []
    for line in text.strip().split('\n'):
        original_line = line  # Keep original for debugging
        line = line.strip()
        
        if line and not line.lstrip().startswith('#'):
            # Clean the line
            domain = line.split()[0]  # Take first word
            domain = domain.strip('*').strip('-').strip()

            if domain and '.' in domain:
                try:
                    companies.append({
                        'name': domain.split('.')[0].replace('-', ' ').title(),
                        'url': f"https://{domain}" if not domain.startswith('http') else domain,
                        'source': 'plain_text',
                        'metadata': {'line': line}
                    })
                except:
                    continue
    return companies

def parse_yaml_companies(text: str) -> List[Dict]:
    """Parse a local YAML file of company endpoints"""
    companies = []
    try:
        data = yaml.safe_load(text)
        if not isinstance(data, list):
            print("    ⚠ companies.yaml must be a list of {name, url} entries")
            return companies
        for item in data:
            if isinstance(item, dict) and item.get('url'):
                companies.append({
                    'name': item.get('name', ''),
                    'url': item['url'],
                    'source': 'yaml_companies',
                    'metadata': {k: v for k, v in item.items() if k not in ('name', 'url')}
                })
    except Exception as e:
        print(f"    ⚠ Error parsing companies.yaml: {e}")
    return companies

def parse_csv_content(csv_text: str) -> List[Dict]:
    """Parse CSV content"""
    companies = []
    try:
        import io
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)

        for row in reader:
            # Try different column names for URL
            url_fields = ['url', 'website', 'domain', 'homepage', 'link', 'URL', 'Website']
            url = None

            for field in url_fields:
                if field in row and row[field]:
                    url = row[field]
                    break

            if url:
                # Get company name
                name_fields = ['name', 'company', 'Name', 'Company', 'title']
                name = ''

                for field in name_fields:
                    if field in row and row[field]:
                        name = row[field]
                        break

                if not name:
                    # Extract from URL
                    try:
                        domain = extract_domain(url)
                        name = domain.split('.')[0].replace('-', ' ').title()
                    except:
                        name = 'Unknown'

                companies.append({
                    'name': name,
                    'url': url if url.startswith('http') else f"https://{url}",
                    'source': 'csv',
                    'metadata': {k: v for k, v in row.items() if k not in url_fields + name_fields}
                })
    except Exception as e:
        print(f"Error parsing CSV: {e}")

    return companies

# ===== WEB SCRAPING PARSERS =====

def scrape_angel_list(html: str) -> List[Dict]:
    """Scrape AngelList company directory"""
    companies = []

    # Simple regex scraping (more robust would use BeautifulSoup)
    company_pattern = r'href="/company/([^"]+)"[^>]*>([^<]+)</a>'
    matches = re.findall(company_pattern, html)

    for company_slug, company_name in matches[:100]:  # Limit to 100
        if company_name and company_slug:
            companies.append({
                'name': company_name.strip(),
                'url': f"https://angel.co/company/{company_slug}",
                'source': 'angel_list',
                'metadata': {'slug': company_slug}
            })

    return companies

def scrape_product_hunt(html: str) -> List[Dict]:
    """Scrape Product Hunt front page"""
    companies = []

    # Look for product links
    product_pattern = r'href="/product/([^"]+)"[^>]*>([^<]+)</a>'
    matches = re.findall(product_pattern, html)

    for product_slug, product_name in matches[:30]:
        if product_name and product_slug:
            companies.append({
                'name': product_name.strip(),
                'url': f"https://www.producthunt.com/products/{product_slug}",
                'source': 'product_hunt',
                'metadata': {'slug': product_slug}
            })

    return companies

def scrape_indie_hackers(html: str) -> List[Dict]:
    """Scrape Indie Hackers products"""
    companies = []

    # Look for product links
    product_pattern = r'href="/product/([^"]+)"[^>]*>([^<]+)</a>'
    matches = re.findall(product_pattern, html)

    for product_slug, product_name in matches[:50]:
        if product_name and product_slug:
            companies.append({
                'name': product_name.strip(),
                'url': f"https://www.indiehackers.com/product/{product_slug}",
                'source': 'indie_hackers',
                'metadata': {'slug': product_slug}
            })

    return companies

# ===== Pre-Fetch ======

def get_already_processed_domains(db_path: Path, source: str = None) -> set:
    """Get domains already processed from specific source"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    
    if source:
        rows = conn.execute(
            "SELECT domain FROM companies WHERE source_name = ?", 
            (source,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT domain FROM companies").fetchall()
    
    conn.close()
    return {row[0] for row in rows}

def get_apollo_pagination_state(db_path: Path) -> dict:
    """Get last pagination state for Apollo to resume from where you left off"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Create metadata table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawler_metadata (
            source_name TEXT PRIMARY KEY,
            last_page INTEGER,
            last_cursor TEXT,
            total_processed INTEGER,
            updated_at TEXT
        )
    """)
    
    result = conn.execute(
        "SELECT last_page, last_cursor, total_processed FROM crawler_metadata WHERE source_name = 'apollo_people'"
    ).fetchone()
    
    conn.close()
    
    if result:
        return {"last_page": result[0], "last_cursor": result[1], "total_processed": result[2]}
    return {"last_page": 0, "last_cursor": None, "total_processed": 0}

def save_apollo_pagination_state(db_path: Path, page: int, cursor: str = None, total: int = None):
    """Save pagination state for Apollo to resume later"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawler_metadata (
            source_name TEXT PRIMARY KEY,
            last_page INTEGER,
            last_cursor TEXT,
            total_processed INTEGER,
            updated_at TEXT
        )
    """)
    conn.execute("""
        INSERT OR REPLACE INTO crawler_metadata (source_name, last_page, last_cursor, total_processed, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, ("apollo_people", page, cursor, total or 0, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

def reset_apollo_pagination(db_path: Path):
    """Reset Apollo pagination to start from page 1"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("DELETE FROM crawler_metadata WHERE source_name = 'apollo_people'")
    conn.commit()
    conn.close()
    print("✓ Apollo pagination reset to page 1")

def show_apollo_status(db_path: Path):
    """Show current Apollo pagination status"""
    state = get_apollo_pagination_state(db_path)
    print(f"Apollo Status:")
    print(f"  Last page: {state.get('last_page', 0)}")
    print(f"  Total processed: {state.get('total_processed', 0)}")
    print(f"  Next page to fetch: {state.get('last_page', 0) + 1}")
   
# ===== DATABASE FETCHER =====

def fetch_from_source(source_id: str, source_config: Dict) -> List[Dict]:
    """Fetch companies from a free source"""
    name = source_config.get('name', source_id)
    enabled = source_config.get('enabled', True)

    if not enabled:
        return []

    print(f"  📡 [{source_id}] {name}")

    try:
        source_type = source_config.get('type', 'json')
        parser_name = source_config.get('parser', 'json')

        parsers = {
            'cncf_landscape': parse_cncf_landscape,
            'github_markdown': parse_github_markdown,
            'hn_whoishiring': parse_hn_whoishiring,
            'plain_text': parse_plain_text,
            'csv': parse_csv_content,
            'rss_feed': parse_rss_feed,
            'yc_json': parse_yc_json,
            'edgar_companies': parse_edgar_companies,
            'opencorporates': parse_opencorporates,
            'sitemap_urls': parse_sitemap_urls,
            'angel_list_scrape': scrape_angel_list,
            'product_hunt_scrape': scrape_product_hunt,
            'indie_hackers_scrape': scrape_indie_hackers,
            'yc_html': scrape_angel_list,
            'crunchbase_sitemap': parse_sitemap_urls,
            'betalist_scrape': scrape_product_hunt,
            'yaml_companies': parse_yaml_companies,
            'apollo_people': parse_apollo_people,
        }

        parser = parsers.get(parser_name)
        if not parser:
            print(f"    ⚠ Unknown parser: {parser_name}")
            return []

        # Handle local files
        if source_type.startswith('local_'):
            file_path = Path(source_config.get('path', ''))
            if not file_path.exists():
                file_path = BASE_DIR / file_path

            if file_path.exists():
                content = file_path.read_text(encoding='utf-8', errors='ignore')
                if source_type == 'local_json':
                    try:
                        data = json.loads(content)
                        companies = parser(data)
                    except json.JSONDecodeError:
                        companies = parser(content)
                else:
                    companies = parser(content)
                print(f"    → Found {len(companies)} companies")
                return companies
            else:
                print(f"    ⚠ File not found: {file_path}")
                return []

        # Handle remote sources
        url = source_config.get('url')
        if not url:
            print(f"    ⚠ No URL specified")
            return []

        time.sleep(random.uniform(1.0, 2.0))

        if source_type in ('xml', 'rss'):
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        elif source_type == 'html':
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        elif source_type == 'yaml_remote':
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        elif source_type == 'apollo':
            if not APOLLO_API_KEY:
                print(f"    ⚠ Missing APOLLO_API_KEY")
                return []

            # Load existing domains to avoid re-enriching
            conn_check = sqlite3.connect(DB_PATH, timeout=30.0)
            existing_domains = {row[0] for row in conn_check.execute("SELECT domain FROM companies").fetchall()}
            conn_check.close()
            print(f"    📋 {len(existing_domains)} domains already in DB — will skip")

            apollo_headers = {
                **HEADERS,
                "X-Api-Key": APOLLO_API_KEY,
                "Content-Type": "application/json",
            }
            
            # Get pagination state
            state = get_apollo_pagination_state(DB_PATH)
            current_page = state.get('last_page', 0) + 1
            total_processed_this_run = state.get('total_processed', 0)
            print(f"    📄 Resuming from page {current_page}, previously processed {total_processed_this_run} contacts")
            
            enriched = []
            seen_emails_this_run = set()
            seen_person_ids = set()
            per_page = 25
            max_pages = 50  # Increased safety limit
            reached_end = False
            

            TITLE_SETS = [
                ["founder", "CEO", "managing director"],
                ["president", "owner", "principal"],
                ["cto", "coo", "cmo", "chief"],
                ["vp", "vice president", "head of"],
                ["partner", "general partner", "managing partner"],
            ]

            FUNDING_SETS = [
                ["seed", "angel"],
                ["series_a"],
                ["series_b"],
                ["series_c", "series_d"],
                [],
            ]

            while len(enriched) < MAX_COMPANIES_PER_DAY and not reached_end and current_page <= max_pages:
                title_set_index = (current_page // 5) % len(TITLE_SETS)
                funding_index = (current_page // 5) % len(FUNDING_SETS)
                current_titles = TITLE_SETS[title_set_index]
                current_funding = FUNDING_SETS[funding_index]

                search_payload = {
                    "person_titles": current_titles,
                    "person_locations": ["United States"],
                    "contact_email_status": ["verified"],
                    "organization_num_employees_ranges": ["1,10", "11,50", "51,200"],
                    "per_page": per_page,
                    "page": current_page,
                }

                if current_funding:
                    search_payload["organization_latest_funding_stage_cd"] = current_funding

                print(f"    🔍 Fetching page {current_page} (titles: {current_titles[0]}...)...")
                print(f"    📊 Funding: {current_funding[0] if current_funding else 'None'}"
)
                response = requests.post(url, json=search_payload, headers=apollo_headers, timeout=30)
                
                if response.status_code != 200:
                    print(f"    ✗ HTTP {response.status_code}: {response.text[:200]}")
                    break

                data = response.json()
                
                # Check pagination info in response
                pagination = data.get('pagination', {})
                total_pages = pagination.get('total_pages', 0)
                
                people = data.get('people', [])
                if not people:
                    print(f"    ✓ No more results at page {current_page}")
                    reached_end = True
                    break

                print(f"    🔍 Page {current_page}/{total_pages}: {len(people)} people, enriching...")

                for person in people:
                    if len(enriched) >= MAX_COMPANIES_PER_DAY:
                        break

                    person_id = person.get('id')
                    if not person_id or person_id in seen_person_ids:
                        continue
                    
                    seen_person_ids.add(person_id)

                    # Skip domains already in DB
                    primary_domain = (person.get('organization') or {}).get('primary_domain', '')
                    if primary_domain:
                        try:
                            check_domain = extract_domain(f"https://{primary_domain}")
                            if check_domain in existing_domains:
                                continue
                        except:
                            pass

                    # Enrich person
                    enrich_response = requests.post(
                        "https://api.apollo.io/api/v1/people/match",
                        json={"id": person_id, "reveal_personal_emails": True},
                        headers=apollo_headers,
                        timeout=30
                    )

                    if enrich_response.status_code != 200:
                        continue

                    enriched_person = enrich_response.json().get('person', {})
                    email = enriched_person.get('email', '')

                    if not email or email in seen_emails_this_run:
                        continue

                    org = enriched_person.get('organization') or {}
                    enriched.append({
                        'first_name': enriched_person.get('first_name', ''),
                        'last_name': enriched_person.get('last_name', ''),
                        'email': email,
                        'title': enriched_person.get('title', ''),
                        'organization': org,
                    })
                    seen_emails_this_run.add(email)
                    print(f"    ✓ [{len(enriched)}/{MAX_COMPANIES_PER_DAY}] {email}")

                    time.sleep(0.5)  # Rate limiting

                # Save progress after each page
                save_apollo_pagination_state(DB_PATH, current_page, None, total_processed_this_run + len(enriched))
                
                # Check if we've reached the last page
                if current_page >= total_pages:
                    print(f"    ✓ Reached last page ({total_pages})")
                    reached_end = True
                else:
                    current_page += 1
                    time.sleep(1.0)  # Polite delay between pages

            # Only reset pagination if we reached the end AND got contacts
            if reached_end and len(enriched) > 0:
                print(f"    ✓ Completed all pages. Resetting pagination for next run.")
                # Comment this out if you want to keep pagination state
                # save_apollo_pagination_state(DB_PATH, 0, None, 0)

            print(f"    ✓ Apollo enrichment complete: {len(enriched)} new contacts found")
            companies = parser({'people': enriched})


        else:  # JSON
            params = source_config.get('params', {})
            response = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                try:
                    data = response.json()
                    companies = parser(data)
                except json.JSONDecodeError:
                    companies = parser(response.text)
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        print(f"    → Found {len(companies)} companies")
        return companies
    
    except Exception as e:
            print(f"    ✗ Error: {e}")
            return []

# ===== MAIN FUNCTIONS =====

def discover_companies_from_yaml_only() -> List[Dict]:
    """Discover companies ONLY from companies.yaml"""
    print("\n" + "=" * 60)
    print("DISCOVERING COMPANIES FROM YAML FILE ONLY")
    print("=" * 60 + "\n")

    sources = load_free_database_sources()

    if 'yaml_companies' not in sources:
        print("❌ 'yaml_companies' source not found in configuration")
        return []

    source_config = sources['yaml_companies']
    print(f"📄 Using source: {source_config.get('name', 'yaml_companies')}")

    companies = fetch_from_source('yaml_companies', source_config)

    unique_companies = []
    seen_domains = set()
    for company in companies:
        try:
            domain = extract_domain(company['url'])
            if domain not in seen_domains:
                seen_domains.add(domain)
                unique_companies.append(company)
        except:
            continue

    print(f"\n{'=' * 60}")
    print(f"📊 YAML FILE COMPANIES FOUND: {len(unique_companies)}")
    print(f"{'=' * 60}\n")

    return unique_companies

# ===== HUNTER.IO INTEGRATION =====

def hunter_domain_search(domain: str) -> dict:
    """Search for emails using Hunter.io API"""
    if not HUNTER_API_KEY:
        raise RuntimeError("Missing HUNTER_API_KEY env var")

    url = "https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": HUNTER_API_KEY}

    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def classify_contact(e: dict) -> int:
    """Classify contact by type with decision makers as highest priority"""
    email = (e.get("value") or "").lower()
    dept = (e.get("department") or "").lower()
    etype = (e.get("type") or "").lower()
    first_name = (e.get("first_name") or "").lower()
    last_name = (e.get("last_name") or "").lower()
    full_name = f"{first_name} {last_name}".lower()
    position = (e.get("position") or "").lower()

    # Combine all text fields for keyword search
    all_text = f"{email} {dept} {full_name} {position}"

    # Priority 0: Decision Makers (highest priority)
    if any(k in all_text for k in DECISION_MAKER_KEYWORDS):
        return 0

    # Priority 1: HR contacts
    if any(k in dept for k in HR_KEYWORDS) or any(k in email for k in HR_KEYWORDS):
        return 1

    # Priority 2: Engineering contacts
    if any(k in dept for k in ENG_KEYWORDS) or any(k in email for k in ENG_KEYWORDS):
        return 2

    # Priority 3: Generic contacts
    if etype == "generic":
        return 3

    # Priority 4: All others (will be filtered out)
    return 4


def extract_ranked_contacts(hunter_response: dict, domain: str) -> tuple[str, list[dict]]:
    """Extract and rank contacts from Hunter.io response with decision makers first"""
    data = hunter_response.get("data", {}) or {}
    organization = data.get("organization") or domain

    ranked = []
    for e in data.get("emails", []) or []:
        email_val = (e.get("value") or "").strip()
        if not email_val:
            continue

        priority = classify_contact(e)

        # Only include decision makers, HR, engineering, and generic contacts
        # (exclude priority 4 - others)
        if priority > 3:
            continue
        if is_generic_email(email_val):
            continue

        name = f"{(e.get('first_name') or '').strip()} {(e.get('last_name') or '').strip()}".strip() or "N/A"

        # Add position if available
        position = (e.get("position") or "").strip()

        ranked.append({
            "email": email_val,
            "name": name,
            "position": position,
            "confidence": e.get("confidence"),
            "type": (e.get("type") or "unknown").lower(),
            "department": (e.get("department") or "").strip(),
            "priority": priority,
            "is_decision_maker": priority == 0,  # Flag for decision makers
        })

    # Sort by: priority (decision makers first), then confidence, then email
    ranked.sort(key=lambda c: (c["priority"], -(c["confidence"] or 0), c["email"].lower()))

    return organization, ranked

def process_companies(companies: List[Dict], max_companies: int = 50):
    print(f"\n{'=' * 60}")
    print(f"PROCESSING UP TO {max_companies} COMPANIES")
    print(f"{'=' * 60}\n")

    results = []

    # FIX: Add timeout and WAL mode
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    
    try:
        processed = {row[0] for row in conn.execute("SELECT domain FROM companies").fetchall()}
        
        unprocessed = []
        for company in companies:
            try:
                domain = extract_domain(company['url'])
                if domain not in processed:
                    unprocessed.append(company)
            except:
                continue
        
        batch = unprocessed[:max_companies]
        print(f"  {len(processed)} already done, {len(unprocessed)} remaining, processing {len(batch)}\n")
        
        for i, company in enumerate(batch, 1):
            try:
                domain = extract_domain(company['url'])
                print(f"[{i}/{len(batch)}] 🔍 {company.get('name', domain)[:40]} ({domain})")
                
                hunter_data = hunter_domain_search(domain)
                organization, contacts = extract_ranked_contacts(hunter_data, domain)
                
                conn.execute(
                    "INSERT OR IGNORE INTO companies (domain, organization) VALUES (?, ?)",
                    (domain, organization if contacts else domain)
                )
                
                # FIX: Commit every 10 companies instead of each
                if i % 10 == 0:
                    conn.commit()
                
                if contacts:
                    print(f"    ✓ Found {len(contacts)} contacts")
                    results.append({
                        "company": company['name'],
                        "domain": domain,
                        "organization": organization,
                        "contacts": contacts
                    })
                else:
                    print(f"    ✗ No contacts found")
                
                time.sleep(CRAWL_DELAY)
                
            except Exception as e:
                print(f"    ✗ Error: {e}")
                conn.rollback()
                time.sleep(CRAWL_DELAY * 2)
        
        conn.commit()  # Final commit
        
    finally:
        conn.close()
    
    # Save results
    if results:
        output_file = BASE_DIR / f"contacts_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Saved {len(results)} companies with contacts to {output_file}")
    
    return results

def init_db():
    with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            domain TEXT UNIQUE,
            organization TEXT,
            category TEXT DEFAULT 'open',
            last_checked TEXT,
            discovered_from TEXT,
            source_name TEXT,
            metadata TEXT
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            email TEXT,
            name TEXT,
            position TEXT,             
            department TEXT,            
            confidence INTEGER,
            type TEXT,
            is_decision_maker INTEGER DEFAULT 0,
            contacted INTEGER DEFAULT 0,
            contacted_at TEXT,
            last_error TEXT,
            retry_count INTEGER DEFAULT 0,
            UNIQUE(company_id, email),
            FOREIGN KEY(company_id) REFERENCES companies(id)
        )
        """)

        # Add missing columns if they don't exist
        columns_to_add = [
            ("companies", "category", "TEXT DEFAULT 'open'"),
            ("contacts", "retry_count", "INTEGER DEFAULT 0"),
            ("contacts", "position", "TEXT"),
            ("contacts", "department", "TEXT"),
            ("contacts", "is_decision_maker", "INTEGER DEFAULT 0"),
        ]
        
        for table, column, col_type in columns_to_add:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        conn.commit()
    print("✅ Database initialized with WAL mode")

def import_json_contacts(json_path: Path):
    data = json.loads(json_path.read_text(encoding="utf-8"))
    
    # FIX: Add timeout and WAL mode
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    
    try:
        for item in data:
            domain = (item.get("domain") or "").strip()
            if not domain:
                continue
            
            conn.execute(
                """
                INSERT OR IGNORE INTO companies 
                (domain, organization, category) 
                VALUES(?, ?, ?)
                """,
                (domain, item.get("organization") or item.get("company") or domain, 'open'),
            )
            
            company_id = conn.execute(
                "SELECT id FROM companies WHERE domain = ?",
                (domain,),
            ).fetchone()[0]
            
            for c in item.get("contacts", []):
                email = (c.get("email") or "").strip()
                if not email:
                    continue
                
                conn.execute(
                    """
                    INSERT OR IGNORE INTO contacts
                    (company_id, email, name, confidence, type, contacted)
                    VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (
                        company_id,
                        email,
                        c.get("name"),
                        c.get("confidence"),
                        c.get("type"),
                    ),
                )
        
        conn.commit()
    finally:
        conn.close()

def process_companies_apollo(companies: List[Dict], max_companies: int = None):
    """Process Apollo results directly into DB — no Hunter call needed"""
    if max_companies is None:
        max_companies = MAX_COMPANIES_PER_DAY

    print(f"\n{'=' * 60}")
    print(f"PROCESSING UP TO {max_companies} APOLLO CONTACTS")
    print(f"{'=' * 60}\n")

    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    results = []

    try:
        processed = {row[0] for row in conn.execute("SELECT domain FROM companies").fetchall()}

        unprocessed = []
        for company in companies:
            try:
                domain = extract_domain(company['url'])
                if domain not in processed:
                    unprocessed.append(company)
            except:
                continue

        batch = unprocessed[:max_companies]
        print(f"  {len(processed)} already done, {len(unprocessed)} remaining, processing {len(batch)}\n")

        for i, company in enumerate(batch, 1):
            try:
                domain = extract_domain(company['url'])
                name = company.get('name', domain)
                meta = company.get('metadata', {})

                contact_email = (meta.get('contact_email') or '').strip()
                contact_name = (meta.get('contact_name') or '').strip()
                contact_title = (meta.get('contact_title') or '').strip()

                print(f"[{i}/{len(batch)}] 🔍 {name[:40]} ({domain})")

                if not contact_email:
                    print(f"    ✗ No email in Apollo response")
                    continue

                if is_generic_email(contact_email):
                    print(f"    ✗ Generic email skipped: {contact_email}")
                    continue

                # Insert company
                conn.execute(
                    "INSERT OR IGNORE INTO companies (domain, organization, source_name) VALUES (?, ?, ?)",
                    (domain, name, 'apollo_people')
                )

                company_id = conn.execute(
                    "SELECT id FROM companies WHERE domain = ?", (domain,)
                ).fetchone()[0]

                # Enforce MAX_CONTACTS_PER_COMPANY
                existing_count = conn.execute(
                    "SELECT COUNT(*) FROM contacts WHERE company_id = ?", (company_id,)
                ).fetchone()[0]
                if existing_count >= MAX_CONTACTS_PER_COMPANY:
                    print(f"    ⏭ Max contacts reached for {domain}")
                    continue

                # Insert contact directly
                conn.execute(
                    """
                    INSERT OR IGNORE INTO contacts
                    (company_id, email, name, position, type, confidence, contacted)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                    """,
                    (company_id, contact_email, contact_name, contact_title, 'personal', 90)
                )

                if i % 10 == 0:
                    conn.commit()

                print(f"    ✓ {contact_email} ({contact_title})")
                results.append({
                    "company": name,
                    "domain": domain,
                    "organization": name,
                    "contacts": [{
                        "email": contact_email,
                        "name": contact_name,
                        "position": contact_title,
                        "type": "personal",
                        "confidence": 90,
                    }]
                })

            except Exception as e:
                print(f"    ✗ Error: {e}")
                conn.rollback()

        conn.commit()

    finally:
        conn.close()

    if results:
        output_file = BASE_DIR / f"contacts_apollo_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Saved {len(results)} Apollo contacts to {output_file}")

    return results

def discover_companies_from_local_file_only() -> List[Dict]:
    """Discover companies ONLY from target_companies.txt"""
    print("\n" + "=" * 60)
    print("DISCOVERING COMPANIES FROM LOCAL FILE ONLY")
    print("=" * 60 + "\n")

    sources = load_free_database_sources()

    if 'local_domains' not in sources:
        print("❌ 'local_domains' source not found in configuration")
        return []

    source_config = sources['local_domains']
    print(f"📄 Using source: {source_config.get('name', 'local_domains')}")

    companies = fetch_from_source('local_domains', source_config)

    unique_companies = []
    seen_domains = set()
    for company in companies:
        try:
            domain = extract_domain(company['url'])
            if domain not in seen_domains:
                seen_domains.add(domain)
                unique_companies.append(company)
        except:
            continue

    print(f"\n{'=' * 60}")
    print(f"📊 LOCAL FILE COMPANIES FOUND: {len(unique_companies)}")
    print(f"{'=' * 60}\n")

    return unique_companies

def discover_companies_from_free_sources() -> List[Dict]:
    """
    Discover companies from ALL enabled free public sources
    """
    print("\n" + "=" * 60)
    print("DISCOVERING COMPANIES FROM FREE PUBLIC SOURCES")
    print("=" * 60 + "\n")

    # Load all free sources
    sources = load_free_database_sources()

    all_companies = []
    enabled_count = 0

    # Count enabled sources
    for sid, config in sources.items():
        if config.get('enabled', True):
            enabled_count += 1

    print(f"📡 Checking {enabled_count} enabled free sources...\n")

    # Process each enabled source
    for i, (source_id, source_config) in enumerate(sources.items(), 1):
        if not source_config.get('enabled', True):
            continue

        name = source_config.get('name', source_id)
        print(f"[{i}/{enabled_count}] {name}")

        companies = fetch_from_source(source_id, source_config)
        all_companies.extend(companies)

        # Small delay between sources
        time.sleep(0.5)

    # Deduplicate by domain
    unique_companies = []
    seen_domains = set()

    for company in all_companies:
        try:
            domain = extract_domain(company['url'])
            if domain not in seen_domains:
                seen_domains.add(domain)
                unique_companies.append(company)
        except:
            continue

    print(f"\n{'=' * 60}")
    print(f"📊 TOTAL UNIQUE COMPANIES FOUND: {len(unique_companies)}")
    print(f"{'=' * 60}\n")

    return unique_companies

def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("FREE PUBLIC DATABASE COMPANY FINDER")
    print("=" * 60 + "\n")

    # 1) Initialize database schema
    init_db()

    # 2) Import contacts from JSON files BEFORE doing anything else
    json_files = list(BASE_DIR.glob("contacts*.json"))
    if json_files:
        # Sort by modification time (newest first)
        json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        latest_file = json_files[0]
        print(f"📥 Importing contacts from: {latest_file.name}")
        import_json_contacts(latest_file)  # <-- Call existing function with Path
    else:
        print("No contacts*.json files found.")

    # 3) Discover companies from free sources
    companies = discover_companies_from_free_sources()
    if not companies:
        print("No companies found. Exiting.")
        return

    # 4) Process companies with Hunter.io (limit to avoid chaos)
    max_to_process = min(50, len(companies))
    process_companies(companies, max_companies=max_to_process)

    print("\n" + "=" * 60)
    print("CRAWLER COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    import sys

    # Check for command-line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--local-only":
        # Run local file only mode
        print("\n" + "=" * 60)
        print("LOCAL FILE ONLY MODE")
        print("=" * 60)

        # 1) Initialize database schema
        init_db()

        # 2) Discover companies from local file only
        companies = discover_companies_from_local_file_only()

        if not companies:
            print("No companies found in local file. Exiting.")
            sys.exit(0)

        # 3) Process companies with Hunter.io
        max_to_process = min(50, len(companies))
        process_companies(companies, max_companies=max_to_process)

        print("\n" + "=" * 60)
        print("LOCAL FILE CRAWLER COMPLETE")
        print("=" * 60)

    elif len(sys.argv) > 1 and sys.argv[1] == "--yaml-only":
        print("\n" + "=" * 60)
        print("YAML FILE ONLY MODE")
        print("=" * 60)

        init_db()

        companies = discover_companies_from_yaml_only()

        if not companies:
            print("No companies found in companies.yaml. Exiting.")
            sys.exit(0)

        max_to_process = min(50, len(companies))
        process_companies(companies, max_companies=max_to_process)

        print("\n" + "=" * 60)
        print("YAML FILE CRAWLER COMPLETE")
        print("=" * 60)

    elif len(sys.argv) > 1 and sys.argv[1] == "--apollo-only":
        print("\n" + "=" * 60)
        print("APOLLO ONLY MODE")
        print("=" * 60)

        init_db()

        sources = load_free_database_sources()
        apollo_config = sources['apollo_people']
        apollo_config['enabled'] = True  # ← add this
        companies = fetch_from_source('apollo_people', apollo_config)

        if not companies:
            print("No contacts returned from Apollo. Exiting.")
            sys.exit(0)

        process_companies_apollo(companies, max_companies=MAX_COMPANIES_PER_DAY)

        print("\n" + "=" * 60)
        print("APOLLO CRAWLER COMPLETE")
        print("=" * 60)

    elif len(sys.argv) > 1 and sys.argv[1] == "--apollo-reset":
        print("\n" + "=" * 60)
        print("RESETTING APOLLO PAGINATION STATE")
        print("=" * 60)
        init_db()
        reset_apollo_pagination(DB_PATH)
        show_apollo_status(DB_PATH)
        sys.exit(0)

    elif len(sys.argv) > 1 and sys.argv[1] == "--apollo-status":
        print("\n" + "=" * 60)
        print("APOLLO PAGINATION STATUS")
        print("=" * 60)
        init_db()
        show_apollo_status(DB_PATH)
        sys.exit(0)

    else:
        # Run normal mode (all sources)
        main()

        