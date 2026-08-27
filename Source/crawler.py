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
MAX_API_CALLS_PER_RUN = int(os.getenv("MAX_API_CALLS_PER_RUN", "100"))

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

HEADERS = {
    "User-Agent": USER_AGENT, 
    "Accept": "application/json",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# Near HR_KEYWORDS and ENG_KEYWORDS, add:
GENERIC_PREFIXES = set()

DECISION_MAKER_KEYWORDS = (
    "ceo", "coo", "cfo", "cto", "founder", "owner",
    "president", "vice president", "director", "executive",
)

DECISION_MAKER_KEYWORDS += (
    "cio", "ciso", "chro", "cro", "svp", "evp",
    "managing director", "managing partner", "partner", "principal"
)

HR_KEYWORDS = (
    "hr", "human resources", "recruiter", "recruiting", "recruitment",
    "talent", "talent acquisition", "people", "people operations",
    "staffing", "sourcer", "careers", "jobs", "hiring"
)

ENG_KEYWORDS = (
    "engineer", "engineering", "developer", "development",
    "technology", "technical", "data", "product"
)

ENG_KEYWORDS += (
    "software", "devops", "platform", "infrastructure",
    "site reliability", "sre"
)

GENERIC_PREFIXES |= {
    "careers", "hr", "recruiting", "hiring", "talent"
}

BLOCKED_PREFIXES = {
    "noreply", "no-reply", "no_reply",
    "donotreply", "do-not-reply", "do_not_reply",
}

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
    "local_txt": {
        "name": "Local Text File",
        "path": "target_companies.txt",
        "type": "local_txt",
        "enabled": False,
        "parser": "plain_text",
        "description": "Your own text file with company URLs (one per line)",
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
        url = org.get('website_url') or ''
        name = org.get('name') or ''
        
        # Debug first person
        if not companies and not url:
            print(f"    🔍 Sample person: {person.get('first_name')} {person.get('last_name')}")
            print(f"    🔍 Org keys: {list(org.keys()) if org else 'NO ORG'}")
            print(f"    🔍 Email: {person.get('email')}")
        
        if not url:
            continue
        
        companies.append({
            'name': name,
            'url': url if url.startswith('http') else f"https://{url}",
            'source': 'apollo_people',
            'metadata': {
                'contact_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                'contact_email': person.get('email', ''),
                'contact_title': person.get('title', '')
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

def save_apollo_pagination_state(db_path: Path, page: int, cursor: str = None, total: int = None, filter_index: int = 0):
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
        """, ("apollo_people", page, f"{cursor}|{filter_index}", total or 0, datetime.now(timezone.utc).isoformat()))
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

def reset_apollo_contacts_to_pending():
    """Reset all Apollo-sourced contacts to pending status"""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Count contacts to reset
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM contacts 
        WHERE type = 'personal' AND contacted = 1
    """)
    count = cur.fetchone()[0]
    
    if count == 0:
        print("✅ No personal contacts with contacted=1 found.")
        conn.close()
        return
    
    print(f"📊 Found {count} personal contacts marked as 'sent'")
    confirm = input(f"Reset {count} contacts to pending? (y/n): ").strip().lower()
    
    if confirm == 'y':
        cur.execute("""
            UPDATE contacts 
            SET contacted = 0, contacted_at = NULL, last_error = NULL 
            WHERE type = 'personal' AND contacted = 1
        """)
        affected = cur.rowcount
        conn.commit()
        print(f"✅ Reset {affected} contacts to pending")
    else:
        print("Cancelled.")
    
    conn.close()

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
    companies = []  # Initialize at the start
    name = source_config.get('name', source_id)
    enabled = source_config.get('enabled', True)

    if not enabled:
        return []

    print(f"  📡 [{source_id}] {name}")

    try:
        source_type = source_config.get('type', 'json')
        parser_name = source_config.get('parser', 'json')

        # Map parser names to functions
        parsers = {
            'cncf_landscape': parse_cncf_landscape,
            'github_markdown': parse_github_markdown,
            'hn_whoishiring': parse_hn_whoishiring,
            'plain_text': parse_plain_text,
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

        # Make request with polite delays
        time.sleep(random.uniform(1.0, 2.0))

        if source_type in ('xml', 'rss'):
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
                print(f"    → Found {len(companies)} companies")
                return companies
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        elif source_type == 'html':
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
                print(f"    → Found {len(companies)} companies")
                return companies
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []
        
        elif source_type == 'yaml_remote':
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
                print(f"    → Found {len(companies)} companies")
                return companies
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        elif source_type in ("markdown", "text"):
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                companies = parser(response.text)
                print(f"    → Found {len(companies)} companies")
                return companies
            else:
                print(f"    ✗ HTTP {response.status_code}")
                return []

        # ===== CLEANED UP APOLLO SECTION =====

        elif source_type == 'apollo':
            if not APOLLO_API_KEY:
                print(f"    ⚠ Missing APOLLO_API_KEY")
                return []

            # Load existing domains
            conn_check = sqlite3.connect(DB_PATH, timeout=30.0)
            existing_domains = {row[0] for row in conn_check.execute("SELECT domain FROM companies").fetchall()}
            conn_check.close()
            print(f"    📋 {len(existing_domains)} domains already in DB — will skip")

            apollo_headers = {
                **HEADERS,
                "X-Api-Key": APOLLO_API_KEY,
                "Content-Type": "application/json",
            }

            # These are Apollo's documented seniority values—not job titles.
            SENIORITY_SETS = [
                ["owner", "founder", "c_suite", "partner"],
                ["vp", "head", "director"],
                ["manager", "senior"],
            ]

            state = get_apollo_pagination_state(DB_PATH)

            try:
                filter_index = int(
                    (state.get("last_cursor") or "0").rsplit("|", 1)[-1]
                )
            except (TypeError, ValueError):
                filter_index = 0

            if not 0 <= filter_index < len(SENIORITY_SETS):
                filter_index = 0
                start_page = 1
            else:
                start_page = max(1, int(state.get("last_page") or 0) + 1)

            all_enriched = []
            raw_enriched_all = [] 
            seen_emails_this_run = set()
            seen_person_ids = set()
            total_processed_this_run = int(state.get("total_processed") or 0)
            credits_used_this_run = 0
            reached_end = False
            max_pages = 20

            for combo_idx in range(filter_index, len(SENIORITY_SETS)):
                if reached_end or len(all_enriched) >= MAX_COMPANIES_PER_DAY:
                    break

                current_seniorities = SENIORITY_SETS[combo_idx]

                print(
                    f"    ▶ Combo {combo_idx + 1}/{len(SENIORITY_SETS)} "
                    f"— seniorities: {current_seniorities}"
                )

                page = start_page if combo_idx == filter_index else 1

                while (
                    page <= max_pages
                    and len(all_enriched) < MAX_COMPANIES_PER_DAY
                    and not reached_end
                ):
                    search_payload = {
                        "person_locations": ["United States"],
                        "person_seniorities": current_seniorities,
                        "page": page,
                        "per_page": 100,
                    }

                    print(
                        f"    🔍 Fetching page {page} "
                        f"for combo {combo_idx + 1}/{len(SENIORITY_SETS)} "
                        f"— seniorities: {current_seniorities}"
                    )

                    try:
                        response = requests.post(
                            url,
                            json=search_payload,
                            headers=apollo_headers,
                            timeout=30,
                        )

                        if response.status_code != 200:
                            print(
                                f"    ✗ HTTP {response.status_code}: "
                                f"{response.text[:300]}"
                            )
                            break

                        data = response.json()
                        people = data.get("people") or []
                        pagination = data.get("pagination") or {}

                        print(
                            f"    📊 Apollo total={pagination.get('total_entries', 0)}, "
                            f"pages={pagination.get('total_pages', 0)}, "
                            f"returned={len(people)}"
                        )

                        if not people:
                            print(
                                f"    ✓ No more results for this combo "
                                f"at page {page}"
                            )
                            break

                        print(f"    🔍 Got {len(people)} people, enriching first...")
                        
                        # ===== STEP 1: ENRICH ALL PEOPLE FIRST =====
                        # Batch enrich in groups of 10 to save credits (Apollo max for batch enrichment) plus credit reporting
                        # No domain is only reported after credit is consuimed
                        batch_size = 10
                        enriched_batch = []
 
                        for i in range(0, len(people), batch_size):
                            if reached_end:
                                break
 
                            batch = people[i:i+batch_size]
                            person_ids = [p.get("id") for p in batch if p.get("id")]
 
                            if not person_ids:
                                continue
 
                            print(f"      → Batch enriching {len(person_ids)} people...")
 
                            try:
                                enrich_response = requests.post(
                                    "https://api.apollo.io/api/v1/people/bulk_match",
                                    json={
                                        "details": [{"id": pid} for pid in person_ids],
                                        "reveal_personal_emails": True
                                    },
                                    headers=apollo_headers,
                                    timeout=30
                                )
                            except Exception as e:
                                print(f"        ❌ Batch error: {e}")
                                continue
                            # Debug enrichment response
                            print(
                                f"        🔍 RAW: "
                                f"{enrich_response.text[:500]}"
                            )

                            if enrich_response.status_code != 200:
                                print(
                                    f"        ❌ Batch enrich failed: "
                                    f"{enrich_response.status_code} — "
                                    f"{enrich_response.text[:300]}"
                                )

                                if enrich_response.status_code == 402:
                                    print(
                                        "        💳 Credit limit reached "
                                        "(Apollo-side)!"
                                    )
                                    reached_end = True

                                break

                            resp_json = enrich_response.json()
                            results = resp_json.get("matches") or []

                            for match in results:
                                print(
                                    f"        🔍 "
                                    f"{match.get('first_name')} "
                                    f"{match.get('last_name')} — "
                                    f"email: {match.get('email')!r}, "
                                    f"linkedin: "
                                    f"{match.get('linkedin_url')!r}"
                                )
 
                            # Apollo bills per record that actually returns enriched data
                            # (i.e. has data, not just an empty stub match). Use presence
                            # of an email or org data as the real credit-consumption signal.
                            records_with_data = sum(
                                1 for r in results
                                if r.get("email") or r.get("organization")
                            )
                            credits_used_this_run += records_with_data
 
                            enriched_batch.extend(results)
                            raw_enriched_all.extend(results)
                            print(f"        ✅ Enriched {len(results)} people "
                                  f"({records_with_data} billed) — "
                                  f"credits used this run: {credits_used_this_run}/{MAX_API_CALLS_PER_RUN}")
                            
                            # Progressive raw export every 50 contacts
                            if len(raw_enriched_all) % 50 == 0:
                                try:
                                    raw_progress_file = (
                                        BASE_DIR
                                        / f"apollo_raw_progress_{len(raw_enriched_all)}_"
                                          f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                                    )
                                    with open(raw_progress_file, "w", encoding="utf-8") as file:
                                        json.dump(raw_enriched_all, file, indent=2, ensure_ascii=False)
                                    print(f"        💾 Progressive raw save: {len(raw_enriched_all)} contacts")
                                except Exception as e:
                                    print(f"        ⚠ Progressive save error: {e}")
                            
                            print(f"        ✅ Enriched {len(results)} people "
                                  f"({records_with_data} billed) — "
                                  f"credits used this run: {credits_used_this_run}/{MAX_API_CALLS_PER_RUN}")
                            
                            # HARD CAP: stop the instant we've hit or exceeded the
                            # configured credit budget, not just "check next loop"
                            if credits_used_this_run >= MAX_API_CALLS_PER_RUN:
                                print(f"    ⚠️ Credit cap reached ({credits_used_this_run}/{MAX_API_CALLS_PER_RUN}). Stopping.")
                                reached_end = True
                                break
 
                            time.sleep(0.5)
 
 
                        print(f"    ✅ Enriched {len(enriched_batch)} people, now filtering...")

                        # ===== STEP 1: SAVE RAW ENRICHED DATA (BEFORE FILTERING) =====
                        # This saves ALL data regardless of MAX_COMPANIES_PER_DAY
                        if enriched_batch:
                            try:
                                # Save page-by-page raw data
                                raw_page_file = (
                                    BASE_DIR
                                    / f"apollo_raw_page_{page}_combo_{combo_idx+1}_"
                                      f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                                )
                                with open(raw_page_file, "w", encoding="utf-8") as file:
                                    json.dump(enriched_batch, file, indent=2, ensure_ascii=False)
                                print(f"    💾 Saved {len(enriched_batch)} RAW enriched contacts from page {page}")
                            except Exception as e:
                                print(f"    ⚠ Error saving raw page data: {e}")
                        
                        # ===== STEP 2: FILTER ENRICHED PEOPLE (RESPECTS MAX_COMPANIES_PER_DAY) =====
                        filtered_this_page = 0
                        for enriched_person in enriched_batch:
                            # Check if we've reached the daily limit for FILTERED contacts
                            if len(all_enriched) >= MAX_COMPANIES_PER_DAY:
                                print(f"    🏆 Daily limit reached: {len(all_enriched)} filtered contacts")
                                break

                            # Skip if already seen
                            person_id = enriched_person.get("id")
                            if not person_id or person_id in seen_person_ids:
                                continue
                            
                            seen_person_ids.add(person_id)
                            
                            # Get organization and domain from enriched data
                            org = enriched_person.get("organization", {})
                            domain = org.get("primary_domain", "")
                            
                            # Skip if no domain
                            if not domain:
                                print(f"      ⏭ {enriched_person.get('first_name', 'Unknown')} - no domain in enriched data")
                                continue
                            
                            # Check if already in DB
                            try:
                                clean_domain = extract_domain(f"https://{domain}")
                                if clean_domain in existing_domains:
                                    print(f"      ⏭ {clean_domain} - already in DB")
                                    continue
                            except:
                                print(f"      ⏭ {domain} - invalid domain")
                                continue
                            
                            # Check if decision maker
                            title = (enriched_person.get("title") or "").lower()
                            decision_makers = ["ceo", "founder", "cto", "vp", "director", "chief", "president", "owner"]
                            if not any(k in title for k in decision_makers):
                                print(f"      ⏭ {enriched_person.get('first_name', 'Unknown')} - not decision maker: {title}")
                                continue
                            
                            # Get email
                            email = enriched_person.get("email", "") or enriched_person.get("personal_email", "")
                            if not email or email in seen_emails_this_run or is_generic_email(email):
                                print(f"      ⏭ {enriched_person.get('first_name', 'Unknown')} - no valid email")
                                continue
                            
                            # Check if we already have this email
                            if email in seen_emails_this_run:
                                continue
                            
                            seen_emails_this_run.add(email)
                            
                            # Add to results (FILTERED contacts - respects MAX_COMPANIES_PER_DAY)
                            all_enriched.append({
                                "first_name": enriched_person.get("first_name", ""),
                                "last_name": enriched_person.get("last_name", ""),
                                "email": email,
                                "title": enriched_person.get("title", ""),
                                "organization": org,
                            })
                            filtered_this_page += 1
                            
                            # Add to existing domains to avoid duplicates in same run
                            try:
                                existing_domains.add(clean_domain)
                            except:
                                pass
                            
                            print(f"    ✓ [{len(all_enriched)}] {email} ({org.get('name', 'Unknown')}) - {domain}")
                        
                        # Save filtered progress after this page
                        if filtered_this_page > 0:
                            save_apollo_pagination_state(
                                DB_PATH,
                                page=page,
                                cursor=None,
                                total=total_processed_this_run + len(all_enriched),
                                filter_index=combo_idx,
                            )
                            
                            # Also save filtered data progressively
                            if all_enriched:
                                try:
                                    filtered_progress_file = (
                                        BASE_DIR
                                        / f"apollo_filtered_progress_{len(all_enriched)}_"
                                          f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                                    )
                                    with open(filtered_progress_file, "w", encoding="utf-8") as file:
                                        json.dump(all_enriched, file, indent=2, ensure_ascii=False)
                                    print(f"    💾 Progressive filtered save: {len(all_enriched)} contacts")
                                except Exception as e:
                                    print(f"    ⚠ Error saving filtered progress: {e}")

                        if len(all_enriched) >= MAX_COMPANIES_PER_DAY:
                            print(f"    🏆 Target reached: {len(all_enriched)} filtered contacts")
                            break

                        if reached_end:
                            print("    ⚠️ Current page processed; stopping at credit cap.")
                            break

                        page += 1
                        time.sleep(CRAWL_DELAY)
                        
                    except Exception as e:
                        print(f"    ✗ Error on page {page}: {e}")
                        break
                
                if len(all_enriched) >= MAX_COMPANIES_PER_DAY:
                    break
            
            # ===== FINAL EXPORTS =====
            
            # 1. Export ALL RAW enriched data (unfiltered, all contacts)
            if raw_enriched_all:
                raw_output_file = (
                    BASE_DIR
                    / f"apollo_raw_enriched_all_"
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                try:
                    with open(raw_output_file, "w", encoding="utf-8") as file:
                        json.dump(raw_enriched_all, file, indent=2, ensure_ascii=False)
                    print(f"    💾 FINAL RAW: Saved {len(raw_enriched_all)} RAW Apollo contacts to {raw_output_file}")
                except Exception as e:
                    print(f"    ⚠ Error saving raw data: {e}")
            else:
                print("    ⚠ No raw Apollo contacts available for export")
                        
            # 2. Export FILTERED data (respects MAX_COMPANIES_PER_DAY)
            if all_enriched:
                filtered_output_file = (
                    BASE_DIR
                    / f"apollo_filtered_final_{len(all_enriched)}_"
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                try:
                    with open(filtered_output_file, "w", encoding="utf-8") as file:
                        json.dump(all_enriched, file, indent=2, ensure_ascii=False)
                    print(f"    💾 FINAL FILTERED: Saved {len(all_enriched)} FILTERED Apollo contacts to {filtered_output_file}")
                except Exception as e:
                    print(f"    ⚠ Error saving filtered data: {e}")
                            
                # Process filtered data into companies format
                companies = parser({"people": all_enriched})
                print(f"    → Found {len(companies)} companies from {len(all_enriched)} filtered contacts")
                            
                # Also save the companies format
                companies_output_file = (
                    BASE_DIR
                    / f"contacts_apollo_{len(companies)}_"
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                try:
                    with open(companies_output_file, "w", encoding="utf-8") as file:
                        json.dump(companies, file, indent=2, ensure_ascii=False)
                    print(f"    💾 Saved {len(companies)} company records to {companies_output_file}")
                except Exception as e:
                    print(f"    ⚠ Error saving companies data: {e}")
                            
                return companies
            else:
                print("    ⚠ No Apollo contacts passed filtering")
                return []

    except Exception as e:
        print(f"    ✗ Error fetching source {source_id}: {e}")
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

def hunter_domain_search(
    domain: str,
    max_contacts: int,
    max_api_calls: int,
) -> tuple[dict, int]:
    """Search Hunter.io with limit/offset pagination."""

    if not HUNTER_API_KEY:
        raise RuntimeError("Missing HUNTER_API_KEY env var")

    url = "https://api.hunter.io/v2/domain-search"
    offset = 0
    api_calls_used = 0
    emails = []
    seen_emails = set()
    merged_response = None

    while len(emails) < max_contacts and api_calls_used < max_api_calls:
        page_limit = min(100, max_contacts - len(emails))

        params = {
            "domain": domain,
            "api_key": HUNTER_API_KEY,
            "limit": page_limit,
            "offset": offset,
        }

        response = requests.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=30,
        )

        api_calls_used += 1
        response.raise_for_status()
        page_response = response.json()

        if merged_response is None:
            merged_response = page_response

        page_emails = (
            (page_response.get("data") or {}).get("emails") or []
        )

        if not page_emails:
            break

        for email_record in page_emails:
            email_value = (
                email_record.get("value") or ""
            ).strip().lower()

            if email_value and email_value not in seen_emails:
                seen_emails.add(email_value)
                emails.append(email_record)

                if len(emails) >= max_contacts:
                    break

        offset += len(page_emails)

        if len(page_emails) < page_limit:
            break

        time.sleep(CRAWL_DELAY)

    if merged_response is None:
        merged_response = {
            "data": {
                "organization": domain,
                "emails": [],
            }
        }
    else:
        merged_response.setdefault("data", {})["emails"] = emails

    return merged_response, api_calls_used


def classify_contact(e: dict) -> int | None:
    """Rank contacts without filtering potentially useful results."""

    email = (e.get("value") or "").lower()
    local_part = email.split("@", 1)[0]

    # Hard exclusion—run before all ranking checks
    if any(prefix in local_part for prefix in BLOCKED_PREFIXES):
        return None

    department = (e.get("department") or "").lower()
    position = (e.get("position") or "").lower()
    email_type = (e.get("type") or "").lower()
    role_text = f"{department} {position}"

    if e.get("decision_maker") is True:
        return 0

    if any(keyword in role_text for keyword in DECISION_MAKER_KEYWORDS):
        return 0

    if (
        any(keyword in role_text for keyword in HR_KEYWORDS)
        or local_part in GENERIC_PREFIXES
    ):
        return 1

    if any(keyword in role_text for keyword in ENG_KEYWORDS):
        return 2

    if email_type == "personal":
        return 3

    if email_type == "generic":
        return 4

    return 5


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
        if priority is None:
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

def ensure_crawler_metadata(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawler_metadata (
            source_name TEXT PRIMARY KEY,
            last_page INTEGER,
            last_cursor TEXT,
            total_processed INTEGER,
            updated_at TEXT
        )
    """)
   
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

                # Insert contact - SET contacted = 0 (pending)
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

        conn.execute("""
        CREATE TABLE IF NOT EXISTS crawler_metadata (
            source_name TEXT PRIMARY KEY,
            last_page INTEGER,
            last_cursor TEXT,
            total_processed INTEGER,
            updated_at TEXT
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

def import_json_contacts(json_file: Path):
    """Import a contacts JSON file directly, without interactive selection."""
    try:
        with open(json_file, "r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data, list):
            print(f"⚠ Skipping {json_file.name}: expected a JSON list")
            return

        imported = 0
        with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
            conn.execute("PRAGMA busy_timeout=30000")
            for company_data in data:
                if not isinstance(company_data, dict):
                    continue

                domain = (company_data.get("domain") or "").strip().lower()
                if not domain and company_data.get("url"):
                    domain = extract_domain(company_data["url"])
                if not domain:
                    continue

                organization = (
                    company_data.get("organization")
                    or company_data.get("company")
                    or domain
                )
                conn.execute(
                    "INSERT OR IGNORE INTO companies (domain, organization, source_name) VALUES (?, ?, ?)",
                    (domain, organization, json_file.name),
                )
                company_id = conn.execute(
                    "SELECT id FROM companies WHERE domain = ?", (domain,)
                ).fetchone()[0]

                for contact in company_data.get("contacts", []) or []:
                    if not isinstance(contact, dict):
                        continue
                    email = (contact.get("email") or "").strip().lower()
                    if not email:
                        continue
                    result = conn.execute(
                        """INSERT OR IGNORE INTO contacts
                        (company_id, email, name, position, department, confidence,
                         type, is_decision_maker, contacted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                        (company_id, email, contact.get("name", ""),
                         contact.get("position", ""), contact.get("department", ""),
                         contact.get("confidence", 70), contact.get("type", "personal"),
                         1 if contact.get("is_decision_maker") else 0),
                    )
                    imported += result.rowcount

        print(f"📥 Imported {imported} contacts from {json_file.name}")
    except Exception as error:
        print(f"❌ Error importing {json_file.name}: {error}")

def import_json_only():
    """Import JSON contacts into database with file selection"""
    print("\n" + "=" * 60)
    print("📥 IMPORT JSON CONTACTS TO DATABASE")
    print("=" * 60)

    json_files = list(BASE_DIR.glob("contacts_apollo*.json")) + list(BASE_DIR.glob("contacts*.json"))
    if not json_files:
        print("❌ No contacts*.json files found!")
        print("💡 Run the crawler first to generate contact files")
        return

    json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    print("\n📄 Available JSON files:")
    for i, json_file in enumerate(json_files[:20], 1):
        mtime = datetime.fromtimestamp(json_file.stat().st_mtime)
        mtime_str = mtime.strftime("%Y-%m-%d %H:%M")
        size = json_file.stat().st_size / 1024
        print(f"   {i}. {json_file.name} ({size:.1f} KB, {mtime_str})")

    print("\n   Enter number to import specific file")
    print("   Press Enter for latest file")
    print("   Type 'all' to import all files")

    file_choice = input("\nSelect file to import: ").strip()

    files_to_import = []

    if file_choice.lower() == 'all':
        files_to_import = json_files
        print(f"\n📋 Will import ALL {len(files_to_import)} files")
    elif file_choice == '':
        files_to_import = [json_files[0]]
        print(f"\n📋 Using latest file: {json_files[0].name}")
    elif file_choice.isdigit():
        idx = int(file_choice) - 1
        if 0 <= idx < len(json_files):
            files_to_import = [json_files[idx]]
            print(f"\n📋 Using file: {json_files[idx].name}")
        else:
            print("❌ Invalid choice, using latest file.")
            files_to_import = [json_files[0]]
    else:
        print("❌ Invalid choice, using latest file.")
        files_to_import = [json_files[0]]

    total_imported = 0
    total_skipped = 0
    total_files = 0

    for json_file in files_to_import:
        print(f"\n{'=' * 60}")
        print(f"📖 Processing: {json_file.name}")
        print(f"{'=' * 60}")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not data:
                print("❌ JSON file is empty!")
                continue

            print(f"📊 Found {len(data)} items in JSON")

            # Convert Apollo metadata format to company format
            converted_data = []
            for item in data:
                # Check if this is Apollo metadata format
                if 'metadata' in item and isinstance(item['metadata'], dict):
                    metadata = item['metadata']
                    
                    # Get domain from URL
                    url = item.get('url', '')
                    domain = ''
                    if url:
                        try:
                            from urllib.parse import urlparse
                            parsed = urlparse(url)
                            domain = parsed.netloc
                        except:
                            domain = url.replace('http://', '').replace('https://', '').split('/')[0]
                    
                    if not domain:
                        continue
                    
                    # Build company record with contacts
                    converted_item = {
                        'domain': domain,
                        'organization': item.get('name', domain),
                        'contacts': []
                    }
                    
                    # Add contact from metadata
                    contact_email = metadata.get('contact_email', '')
                    if contact_email:
                        converted_item['contacts'].append({
                            'email': contact_email,
                            'name': metadata.get('contact_name', ''),
                            'position': metadata.get('contact_title', ''),
                            'confidence': 90,
                            'type': 'personal',
                            'is_decision_maker': True
                        })
                    
                    if converted_item['contacts']:
                        converted_data.append(converted_item)
                else:
                    # Already in company format
                    converted_data.append(item)

            print(f"✅ {len(converted_data)} companies with contacts after conversion")

            if not converted_data:
                print("❌ No companies with contacts found in JSON!")
                continue

            conn = sqlite3.connect(DB_PATH)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            cursor = conn.cursor()

            imported = 0
            skipped = 0

            for company_data in converted_data:
                domain = (company_data.get('domain', '') or '').strip().lower()
                if not domain:
                    print(f"   ⚠ Skipping item with no domain: {company_data.get('company', 'Unknown')}")
                    continue

                org_name = company_data.get('organization', '') or company_data.get('company', '') or domain

                cursor.execute("""
                    INSERT OR IGNORE INTO companies (domain, organization, source_name)
                    VALUES (?, ?, ?)
                """, (domain, org_name, json_file.name))

                result = cursor.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
                if result is None:
                    print(f"   ⚠ Could not insert/find company: {domain}")
                    continue

                company_id = result[0]

                contacts = company_data.get('contacts', [])
                for contact in contacts:
                    email = (contact.get('email', '') or '').strip().lower()
                    if not email:
                        continue

                    cursor.execute("""
                        SELECT id FROM contacts 
                        WHERE company_id = ? AND email = ?
                    """, (company_id, email))

                    if cursor.fetchone():
                        skipped += 1
                        continue

                    cursor.execute("""
                        INSERT INTO contacts
                        (company_id, email, name, position, confidence, type, is_decision_maker, contacted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """, (
                        company_id,
                        email,
                        (contact.get('name', '') or '').strip(),
                        (contact.get('position', '') or '').strip(),
                        contact.get('confidence', 70),
                        contact.get('type', 'personal'),
                        1 if contact.get('is_decision_maker') else 0,
                    ))
                    imported += 1

                if imported % 10 == 0:
                    conn.commit()
                    print(f"   📊 Progress: {converted_data.index(company_data) + 1}/{len(converted_data)} companies, {imported} contacts imported")

            conn.commit()
            conn.close()

            print(f"\n   ✅ Added {len(converted_data)} companies")
            print(f"   📥 Imported: {imported} new contacts")
            print(f"   ⏭️  Skipped: {skipped} duplicates")

            total_imported += imported
            total_skipped += skipped
            total_files += 1

        except Exception as e:
            print(f"❌ Error importing {json_file.name}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'=' * 60}")
    print("📊 IMPORT SUMMARY")
    print(f"{'=' * 60}")
    print(f"📁 Files processed: {total_files}")
    print(f"📥 New contacts imported: {total_imported}")
    print(f"⏭️  Duplicates skipped: {total_skipped}")
    print(f"💾 Database: {DB_PATH.name}")
    print(f"{'=' * 60}")

    if total_imported > 0:
        csv_file = BASE_DIR / f"imported_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['email', 'name', 'domain', 'organization', 'type', 'imported_at'])
            
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT c.email, c.name, co.domain, co.organization, c.type, c.contacted_at
                FROM contacts c
                JOIN companies co ON c.company_id = co.id
                ORDER BY co.domain, c.email
                LIMIT 1000
            """)
            for row in cursor.fetchall():
                writer.writerow(row)
            conn.close()
        
        print(f"📄 CSV saved: {csv_file.name} (first 1000 contacts)")

def process_companies(
    companies: List[Dict],
    max_companies: int = MAX_COMPANIES_PER_DAY,
    state_name: str = "hunter_all",
):
    """Process companies and fetch contacts through Hunter.io."""
    
    # ===== FIRST: Check if we already have contacts from Apollo =====
    conn_check = sqlite3.connect(DB_PATH, timeout=30.0)
    conn_check.execute("PRAGMA journal_mode=WAL")
    
    # Get all companies that already have contacts
    existing_contacts = conn_check.execute("""
        SELECT DISTINCT co.domain 
        FROM companies co
        INNER JOIN contacts c ON c.company_id = co.id
    """).fetchall()
    
    existing_domains_with_contacts = {row[0] for row in existing_contacts}
    conn_check.close()
    
    print(f"📊 {len(existing_domains_with_contacts)} companies already have contacts in DB")
    
    # Filter out companies that already have contacts
    companies_to_process = []
    for company in companies:
        try:
            domain = extract_domain(company["url"])
            if domain not in existing_domains_with_contacts:
                companies_to_process.append(company)
            else:
                print(f"   ⏭ Skipping {domain} - already has contacts")
        except:
            continue
    
    print(f"📊 {len(companies_to_process)} companies need Hunter.io processing")
    
    if not companies_to_process:
        print("✅ All companies already have contacts. Skipping Hunter.io.")
        return []
    
    # ===== Continue with Hunter.io for companies that need it =====
    hunter_state = get_hunter_pagination_state(
        DB_PATH,
        state_name,
    )

    start_index = hunter_state["companies_completed"]
    total_contacts_collected = hunter_state["total_contacts"]
    saved_last_domain = hunter_state["last_domain"]

    # Check if we've already processed all companies
    if start_index >= len(companies_to_process):
        print(f"✅ All {len(companies_to_process)} companies already processed. Nothing to do.")
        return []

    batch_changed = False

    if start_index > 0:
        if start_index > len(companies_to_process):
            batch_changed = True
        else:
            previous_company = companies_to_process[start_index - 1]
            try:
                previous_domain = extract_domain(previous_company["url"])
                if previous_domain != saved_last_domain:
                    batch_changed = True
            except:
                batch_changed = True

    if batch_changed:
        print("🔄 New or changed company batch detected; resetting cursor.")
        start_index = 0
        total_contacts_collected = 0

        with sqlite3.connect(DB_PATH, timeout=30.0) as state_conn:
            state_conn.execute(
                "DELETE FROM crawler_metadata WHERE source_name = ?",
                (state_name,),
            )

    results = []
    total_api_calls_used = 0
    companies_processed_this_run = 0

    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    try:
        # Calculate how many companies we can process (respecting max_companies)
        remaining_companies = len(companies_to_process) - start_index
        companies_to_process_batch = min(max_companies, remaining_companies)
        
        batch = companies_to_process[start_index:start_index + companies_to_process_batch]
        print(f"  processing {len(batch)} companies (starting at index {start_index + 1})\n")

        for i, company in enumerate(batch, start_index + 1):
            if total_api_calls_used >= MAX_API_CALLS_PER_RUN:
                print(
                    f"⚠️ Hunter API limit reached "
                    f"({total_api_calls_used}/"
                    f"{MAX_API_CALLS_PER_RUN})"
                )
                break
            
            try:
                domain = extract_domain(company["url"])
                company_name = company.get("name") or domain

                print(
                    f"[{i}/{len(companies_to_process)}] 🔍 "
                    f"{company_name} ({domain})"
                )

                hunter_data, api_calls_used = hunter_domain_search(
                    domain,
                    max_contacts=MAX_CONTACTS_PER_COMPANY,
                    max_api_calls=(
                        MAX_API_CALLS_PER_RUN - total_api_calls_used
                    ),
                )

                total_api_calls_used += api_calls_used

                organization, contacts = extract_ranked_contacts(
                    hunter_data,
                    domain,
                )

                # Only insert if we have contacts
                if contacts:
                    conn.execute(
                        "INSERT OR IGNORE INTO companies (domain, organization) VALUES (?, ?)",
                        (domain, organization if contacts else domain),
                    )

                    total_contacts_collected += len(contacts)
                    companies_processed_this_run += 1

                    save_hunter_pagination_state(
                        conn,
                        state_name=state_name,
                        companies_completed=i,
                        last_domain=domain,
                        total_contacts=total_contacts_collected,
                    )

                    print(
                        f"    ✓ Found {len(contacts)} contacts "
                        f"(used {api_calls_used} API call(s))"
                    )

                    results.append({
                        "company": company_name,
                        "domain": domain,
                        "organization": organization,
                        "contacts": contacts,
                    })
                else:
                    print(
                        f"    ✗ No contacts found "
                        f"(used {api_calls_used} API call(s))"
                    )

                if i % 10 == 0:
                    conn.commit()

                time.sleep(CRAWL_DELAY)

            except Exception as e:
                print(f"    ✗ Error: {e}")
                conn.rollback()
                time.sleep(CRAWL_DELAY * 2)

        conn.commit()  # Final commit

        final_state = get_hunter_pagination_state(
            DB_PATH,
            state_name,
        )

        print("\n" + "=" * 60)
        print("HUNTER RUN SUMMARY")
        print("=" * 60)
        print(f"Companies processed this run: {companies_processed_this_run}")
        print(
            f"Companies completed overall: "
            f"{final_state['companies_completed']}/{len(companies_to_process)}"
        )
        print(f"Contacts collected overall: {final_state['total_contacts']}")
        print(f"API calls used this run: {total_api_calls_used}")
        print("=" * 60)

    finally:
        conn.close()
    
    # Save results
    if results:
        output_file = BASE_DIR / f"contacts_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Saved {len(results)} companies with contacts to {output_file}")
    
    return results

def get_hunter_pagination_state(
    db_path: Path,
    state_name: str = "hunter_all",
) -> dict:
    with sqlite3.connect(db_path, timeout=30.0) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        ensure_crawler_metadata(conn)

        row = conn.execute(
            """
            SELECT last_page, last_cursor, total_processed, updated_at
            FROM crawler_metadata
            WHERE source_name = ?
            """,
            (state_name,),
        ).fetchone()

    if not row:
        return {
            "companies_completed": 0,
            "last_domain": None,
            "total_contacts": 0,
            "updated_at": None,
        }

    return {
        "companies_completed": row[0] or 0,
        "last_domain": row[1],
        "total_contacts": row[2] or 0,
        "updated_at": row[3],
    }


def save_hunter_pagination_state(
    conn,
    state_name: str,
    companies_completed: int,
    last_domain: str,
    total_contacts: int,
):
    ensure_crawler_metadata(conn)

    conn.execute(
        """
        INSERT OR REPLACE INTO crawler_metadata
        (source_name, last_page, last_cursor, total_processed, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            state_name,
            companies_completed,
            last_domain,
            total_contacts,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def reset_hunter_pagination(db_path: Path):
    with sqlite3.connect(db_path, timeout=30.0) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        ensure_crawler_metadata(conn)
        conn.execute(
            "DELETE FROM crawler_metadata WHERE source_name LIKE 'hunter_%'"
        )

    print("✓ Hunter pagination reset")


def show_hunter_status(db_path: Path):
    modes = (
        ("All sources", "hunter_all"),
        ("Local file", "hunter_local"),
        ("YAML file", "hunter_yaml"),
    )

    print("Hunter Status:")

    for label, state_name in modes:
        state = get_hunter_pagination_state(db_path, state_name)

        print(f"\n  {label}:")
        print(
            f"    Companies completed: "
            f"{state['companies_completed']}"
        )
        print(
            f"    Next company position: "
            f"{state['companies_completed'] + 1}"
        )
        print(f"    Last domain: {state['last_domain'] or 'N/A'}")
        print(f"    Contacts collected: {state['total_contacts']}")
        print(f"    Last updated: {state['updated_at'] or 'N/A'}")

def discover_companies_from_local_file_only() -> List[Dict]:
    """Discover companies ONLY from target_companies.txt"""
    print("\n" + "=" * 60)
    print("DISCOVERING COMPANIES FROM LOCAL FILE ONLY")
    print("=" * 60 + "\n")

    sources = load_free_database_sources()

    if 'local_txt' not in sources:
        print("❌ 'local_txt' source not found in configuration")
        return []

    source_config = sources['local_txt']
    print(f"📄 Using source: {source_config.get('name', 'local_txt')}")

    companies = fetch_from_source('local_txt', source_config)

    unique_companies = []
    seen_domains = set()
    for company in companies or []:
        url = company.get('url', '')
        if not url:
            continue

        try:
            domain = extract_domain(url)
        except Exception:
            continue

        if domain in seen_domains:
            continue

        seen_domains.add(domain)
        company['source_name'] = 'local_txt'
        unique_companies.append(company)

    print(f"\n{'=' * 60}")
    print(f"📊 LOCAL FILE COMPANIES FOUND: {len(unique_companies)}")
    print(f"{'=' * 60}\n")

    return unique_companies


def discover_companies_from_free_sources() -> List[Dict]:
    """Discover companies from all enabled free sources."""
    print("\n" + "=" * 60)
    print("DISCOVERING COMPANIES FROM FREE SOURCES")
    print("=" * 60 + "\n")

    sources = load_free_database_sources()
    enabled_sources = [sid for sid, cfg in sources.items() if cfg.get('enabled', False)]

    if not enabled_sources:
        print("No enabled free sources found. Check your YAML overrides or enable sources in configuration.")
        return []

    unique_companies = []
    seen_domains = set()

    for source_id in enabled_sources:
        source_config = sources[source_id]
        fetched = fetch_from_source(source_id, source_config) or []

        for company in fetched:
            url = company.get('url', '')
            if not url:
                continue
            try:
                domain = extract_domain(url)
            except Exception:
                continue

            if domain in seen_domains:
                continue

            seen_domains.add(domain)
            company['source_name'] = source_id
            unique_companies.append(company)

    print(f"\n{'=' * 60}")
    print(f"📊 FREE SOURCE COMPANIES FOUND: {len(unique_companies)}")
    print(f"{'=' * 60}\n")

    return unique_companies


def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("FREE PUBLIC DATABASE COMPANY FINDER")
    print("=" * 60 + "\n")

    # 1) Initialize database schema
    init_db()

    # 2) Import ALL existing JSON contacts before running
    json_files = list(BASE_DIR.glob("contacts*.json"))
    if json_files:
        json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        print(f"📥 Found {len(json_files)} JSON files to import")
        for json_file in json_files:
            import_json_contacts(json_file)
    else:
        print("No contacts*.json files found.")

    # 3) Discover companies from free sources
    companies = discover_companies_from_free_sources()
    if not companies:
        print("No companies found. Exiting.")
        return

    # 4) Process companies with Hunter.io
    max_to_process = min(MAX_COMPANIES_PER_DAY, len(companies))
    process_companies(
        companies,
        max_companies=max_to_process,
        state_name="hunter_all",
    )

    # 5) Import ANY new JSON files after processing
    json_files_after = list(BASE_DIR.glob("contacts*.json"))
    if json_files_after:
        json_files_after.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        # Check if there are files newer than the import
        if json_files_after:
            print(f"\n📥 Final import of any new contacts...")
            for json_file in json_files_after:
                # Check if this file was already imported (optional)
                import_json_contacts(json_file)

    print("\n" + "=" * 60)
    print("CRAWLER COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    import sys

    # Check for command-line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--local-only":
        init_db()

        companies = discover_companies_from_local_file_only()

        if not companies:
            print("No companies found in local file. Exiting.")
            sys.exit(0)

        max_to_process = min(
            MAX_COMPANIES_PER_DAY,
            len(companies),
        )

        process_companies(
            companies,
            max_companies=max_to_process,
            state_name="hunter_all",
        )

        print("\nLOCAL FILE CRAWLER COMPLETE")
        print("=" * 60)

    elif len(sys.argv) > 1 and sys.argv[1] == "--yaml-only":
        init_db()

        companies = discover_companies_from_yaml_only()

        if not companies:
            print("No companies found in companies.yaml. Exiting.")
            sys.exit(0)

        max_to_process = min(
            MAX_COMPANIES_PER_DAY,
            len(companies),
        )

        process_companies(
            companies,
            max_companies=max_to_process,
            state_name="hunter_yaml",
        )

        print("\nYAML FILE CRAWLER COMPLETE")

    elif len(sys.argv) > 1 and sys.argv[1] == "--apollo-only":
        print("\n" + "=" * 60)
        print("APOLLO ONLY MODE")
        print("=" * 60)

        init_db()
        
        # Import existing JSON files first
        json_files = list(BASE_DIR.glob("contacts*.json"))
        if json_files:
            json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for json_file in json_files:
                print(f"📥 Importing contacts from: {json_file.name}")
                import_json_contacts(json_file)

        sources = load_free_database_sources()
        apollo_config = sources['apollo_people']
        apollo_config['enabled'] = True
        companies = fetch_from_source('apollo_people', apollo_config)

        if not companies:
            print("No contacts returned from Apollo. Exiting.")
            sys.exit(0)

        process_companies_apollo(companies, max_companies=MAX_COMPANIES_PER_DAY)
        
        # Import Apollo results after run
        json_files_after = list(BASE_DIR.glob("contacts_apollo*.json"))
        if json_files_after:
            json_files_after.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for json_file in json_files_after:
                print(f"\n📥 Importing Apollo results from: {json_file.name}")
                import_json_contacts(json_file)

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

    elif len(sys.argv) > 1 and sys.argv[1] == "--hunter-reset":
        print("\nRESETTING HUNTER PAGINATION STATE")
        init_db()
        reset_hunter_pagination(DB_PATH)
        show_hunter_status(DB_PATH)
        sys.exit(0)

    elif len(sys.argv) > 1 and sys.argv[1] == "--hunter-status":
        print("\nHUNTER PAGINATION STATUS")
        init_db()
        show_hunter_status(DB_PATH)
        sys.exit(0)

    else:
        # Run normal mode (all sources)
        main()