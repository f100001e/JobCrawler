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
DATABASE_SOURCES_FILE = BASE_DIR / os.getenv("DATABASE_SOURCES_FILE", "free_databases.yaml")

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
USER_AGENT = "MetaCrawler/1.0 (+polite; research)"
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY_SECONDS", "2.0"))  # Be extra polite with free sources

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

# ===== FREE PUBLIC DATABASE SOURCES =====
def load_free_database_sources() -> Dict:
    """Load FREE public database sources that require NO API keys.
    YAML is treated as overrides (enabled/path/params/etc) unless it defines a brand-new source.
    """

    defaults = {
        # ===== Y COMBINATOR SOURCES =====
        "yc_export": {
            "name": "Y Combinator Companies Export",
            "url": "https://www.ycombinator.com/companies/export.json",
            "type": "json",
            "enabled": True,
            "parser": "yc_json",
            "description": "Official YC company list (free, no auth)",
            "estimated_companies": 4000,
        },
        "yc_companies_page": {
            "name": "YC Companies HTML Page",
            "url": "https://www.ycombinator.com/companies",
            "type": "html",
            "enabled": True,
            "parser": "yc_html",
            "description": "YC companies directory page",
            "estimated_companies": 100,
        },

        # ===== GITHUB DATASETS (FREE, PUBLIC) =====
        "github_yc_dataset": {
            "name": "GitHub YC Dataset",
            "url": "https://raw.githubusercontent.com/saasify-sh/awesome-yc-companies/master/README.md",
            "type": "markdown",
            "enabled": True,
            "parser": "github_markdown",
            "description": "Awesome YC Companies list on GitHub",
            "estimated_companies": 300,
        },
        "github_startup_resources": {
            "name": "GitHub Startup Resources",
            "url": "https://raw.githubusercontent.com/mmccaff/PlacesToPostYourStartup/master/README.md",
            "type": "markdown",
            "enabled": True,
            "parser": "github_markdown",
            "description": "Places to post your startup",
            "estimated_companies": 200,
        },
        "github_awesome_startups": {
            "name": "GitHub Awesome Startups",
            "url": "https://raw.githubusercontent.com/atinfo/awesome-startups/master/README.md",
            "type": "markdown",
            "enabled": True,
            "parser": "github_markdown",
            "description": "Curated list of awesome startups",
            "estimated_companies": 150,
        },

        # ===== PUBLIC API DATASETS (NO AUTH) =====
        "public_apis_org": {
            "name": "Public APIs Directory",
            "url": "https://api.publicapis.org/entries",
            "type": "json",
            "enabled": True,
            "parser": "public_apis",
            "description": "Companies with public APIs",
            "estimated_companies": 1000,
        },

        # ===== TECH NEWS/RSS FEEDS =====
        "techcrunch_feed": {
            "name": "TechCrunch RSS Feed",
            "url": "https://techcrunch.com/feed/",
            "type": "rss",
            "enabled": True,
            "parser": "rss_feed",
            "description": "TechCrunch articles mentioning companies",
            "estimated_companies": 50,
        },
        "hacker_news_whoishiring": {
            "name": "Hacker News Who is Hiring",
            "url": "https://hn.algolia.com/api/v1/search?tags=story,author_whoishiring",
            "type": "json",
            "enabled": True,
            "parser": "hn_whoishiring",
            "description": "HN Who is Hiring posts (mentions companies)",
            "estimated_companies": 1000,
        },

        # ===== GOVERNMENT/OPEN DATA =====
        "edgar_companies": {
            "name": "SEC EDGAR Company List",
            "url": "https://www.sec.gov/files/company_tickers.json",
            "type": "json",
            "enabled": True,
            "parser": "edgar_companies",
            "description": "All companies registered with SEC (US public companies)",
            "estimated_companies": 8000,
        },

        # ===== LOCAL FILES (USER PROVIDED) =====
        "local_domains": {
            "name": "Local Domains File",
            "path": "companies.txt",
            "type": "local_txt",
            "enabled": True,
            "parser": "plain_text",
            "description": "Your own list of target domains",
            "estimated_companies": "variable",
        },
        "local_csv": {
            "name": "Local CSV File",
            "path": "companies.csv",
            "type": "local_csv",
            "enabled": False,
            "parser": "csv",
            "description": "Your own CSV with company data",
            "estimated_companies": "variable",
        },

        # ===== (OPTIONAL / OFTEN BLOCKED) =====
        # Keep these in defaults so YAML can enable/disable them, but expect blocks.
        "crunchbase_open_data": {
            "name": "Crunchbase Open Data Map",
            "url": "https://data.crunchbase.com/docs/open-data-map",
            "type": "html",
            "enabled": False,
            "parser": "crunchbase_sitemap",
            "description": "Crunchbase sitemap for company discovery",
            "estimated_companies": 500,
        },
        "opencorporates": {
            "name": "OpenCorporates API",
            "url": "https://api.opencorporates.com/v0.4/companies/search",
            "type": "json",
            "enabled": False,
            "params": {"q": "technology", "per_page": 100},
            "parser": "opencorporates",
            "description": "Global corporate data (often auth / limited)",
            "estimated_companies": 100,
        },
        "yellowpages_sitemap": {
            "name": "YellowPages Sitemap",
            "url": "https://www.yellowpages.com/sitemap.xml",
            "type": "xml",
            "enabled": False,
            "parser": "sitemap_urls",
            "description": "YellowPages business listings (often blocked)",
            "estimated_companies": 100,
        },
        "angel_list_public": {
            "name": "AngelList Public Pages",
            "url": "https://angel.co/companies",
            "type": "html",
            "enabled": False,
            "parser": "angel_list_scrape",
            "description": "AngelList directory (often blocked / JS-heavy)",
            "estimated_companies": 200,
        },
        "product_hunt_public": {
            "name": "Product Hunt Today",
            "url": "https://www.producthunt.com/",
            "type": "html",
            "enabled": False,
            "parser": "product_hunt_scrape",
            "description": "Product Hunt front page (often blocked / JS-heavy)",
            "estimated_companies": 30,
        },
        "indie_hackers": {
            "name": "Indie Hackers Products",
            "url": "https://www.indiehackers.com/products",
            "type": "html",
            "enabled": False,
            "parser": "indie_hackers_scrape",
            "description": "Indie Hackers product directory (often blocked / JS-heavy)",
            "estimated_companies": 500,
        },
        "betalist": {
            "name": "BetaList Startups",
            "url": "https://betalist.com/",
            "type": "html",
            "enabled": False,
            "parser": "betalist_scrape",
            "description": "BetaList startup directory (often blocks bots)",
            "estimated_companies": 100,
        },
    }

    # Apply YAML overrides
    if DATABASE_SOURCES_FILE.exists():
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

# ===== FREE PARSER FUNCTIONS (NO API KEYS NEEDED) =====

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

def parse_public_apis(data: Any) -> List[Dict]:
    """Parse public-apis.org data"""
    companies = []
    if isinstance(data, dict):
        entries = data.get('entries', [])
        for entry in entries:
            if entry.get('Link'):
                companies.append({
                    'name': entry.get('API', ''),
                    'url': entry['Link'],
                    'source': 'public_apis',
                    'metadata': entry
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
        line = line.strip()
        if line and not line.startswith('#'):
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

# ===== DATABASE FETCHER =====

def fetch_from_free_source(source_id: str, source_config: Dict) -> List[Dict]:
    """Fetch companies from a free source"""
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
            'yc_json': parse_yc_json,
            'public_apis': parse_public_apis,
            'edgar_companies': parse_edgar_companies,
            'opencorporates': parse_opencorporates,
            'hn_whoishiring': parse_hn_whoishiring,
            'sitemap_urls': parse_sitemap_urls,
            'rss_feed': parse_rss_feed,
            'github_markdown': parse_github_markdown,
            'plain_text': parse_plain_text,
            'csv': parse_csv_content,
            'angel_list_scrape': scrape_angel_list,
            'product_hunt_scrape': scrape_product_hunt,
            'indie_hackers_scrape': scrape_indie_hackers,
            'yc_html': scrape_angel_list,  # Reuse for now
            'crunchbase_sitemap': parse_sitemap_urls,
            'betalist_scrape': scrape_product_hunt  # Reuse for now
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

        if source_type == 'xml' or source_type == 'rss':
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

def discover_companies_from_free_sources() -> List[Dict]:
    """Discover companies from all free public sources"""
    print("\n" + "=" * 60)
    print("DISCOVERING COMPANIES FROM FREE PUBLIC SOURCES")
    print("=" * 60 + "\n")

    # Load free sources
    sources = load_free_database_sources()

    # Show enabled sources
    enabled_sources = {k: v for k, v in sources.items() if v.get('enabled', True)}
    print(f"Enabled sources: {len(enabled_sources)}/{len(sources)}")

    all_companies = []

    for source_id, source_config in enabled_sources.items():
        companies = fetch_from_free_source(source_id, source_config)
        all_companies.extend(companies)

        # Be extra polite between sources
        time.sleep(random.uniform(1.0, 3.0))

    # Deduplicate
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
    """Process companies with Hunter.io"""
    print(f"\n{'=' * 60}")
    print(f"PROCESSING UP TO {max_companies} COMPANIES")
    print(f"{'=' * 60}\n")

    results = []

    with sqlite3.connect(DB_PATH) as conn:
        for i, company in enumerate(companies[:max_companies], 1):
            try:
                domain = extract_domain(company['url'])
                print(f"[{i}/{len(companies[:max_companies])}] 🔍 {company.get('name', domain)[:40]} ({domain})")

                # Skip if already processed recently (you'd add this check)

                # Get contacts
                hunter_data = hunter_domain_search(domain)
                organization, contacts = extract_ranked_contacts(hunter_data, domain)

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

                # Be polite to Hunter.io API
                time.sleep(CRAWL_DELAY)

            except Exception as e:
                print(f"    ✗ Error: {e}")
                time.sleep(CRAWL_DELAY * 2)

    # Save results
    if results:
        output_file = BASE_DIR / f"contacts_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Saved {len(results)} companies with contacts to {output_file}")

    return results

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            domain TEXT UNIQUE,
            organization TEXT,
            category TEXT DEFAULT 'engineering',
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
            confidence INTEGER,
            type TEXT,
            contacted INTEGER DEFAULT 0,
            contacted_at TEXT,
            last_error TEXT,
            retry_count INTEGER DEFAULT 0,
            UNIQUE(company_id, email),
            FOREIGN KEY(company_id) REFERENCES companies(id)
        )
        """)

        # Add missing columns if they don't exist
        try:
            conn.execute("ALTER TABLE companies ADD COLUMN category TEXT DEFAULT 'engineering'")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            conn.execute("ALTER TABLE contacts ADD COLUMN retry_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

        conn.commit()
    print("Database initialized")

def import_json_contacts(json_path: Path):
    data = json.loads(json_path.read_text(encoding="utf-8"))

    with sqlite3.connect(DB_PATH) as conn:
        for item in data:
            domain = (item.get("domain") or "").strip()
            if not domain:
                continue

            # Insert company with default category
            conn.execute(
                """
                INSERT OR IGNORE INTO companies 
                (domain, organization, category) 
                VALUES(?, ?, ?)
                """,
                (domain, item.get("organization") or item.get("company") or domain, 'engineering'),
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
    main();