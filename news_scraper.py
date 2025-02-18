import requests
from bs4 import BeautifulSoup
import gspread
import re
import datetime
import random
import time
import signal
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Configuration
SERVICE_ACCOUNT_FILE = "your_json_fie_path"
SPREADSHEET_ID = "your_google_spreadsheet_id"
MAX_ARTICLE_AGE_HOURS = 24
REQUEST_TIMEOUT = 30
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
]

# Regex Patterns
FINTECH_KEYWORDS = re.compile(
    r'\b(?:fintech|financial[ _-]?tech|digital[ _-]?(?:bank(?:ing)?|wallet|pay(?:ments?)|lending)|'
    r'crypto(?:currenc(?:y|ies))?|blockchain|defi|insurtech|wealth[ _-]?management|'
    r'open[ _-]?banking|regtech|paytech|robo[ _-]?advisor|neo[ _-]?bank)\b',
    re.IGNORECASE
)

HRTECH_KEYWORDS = re.compile(
    r'\b(?:hr[ _-]?tech|human[ _-]?resources[ _-]?tech|workforce[ _-]?(?:management|analytics)|'
    r'talent[ _-]?(?:acquisition|management)|ATS|HRIS|HCM|payroll[ _-]?(?:processing|automation)|'
    r'employee[ _-]?(?:engagement|experience)|workday|bamboohr|gusto|zenefits)\b',
    re.IGNORECASE
)

# Global scheduler instance
scheduler = BlockingScheduler()

def signal_handler(sig, frame):
    """Handle graceful shutdown"""
    print("\n🛑 Received shutdown signal. Stopping scheduler...")
    scheduler.shutdown()
    exit(0)

def setup_requests_session():
    """Configure HTTP session with retry logic"""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1'
    })
    return session

def create_hyperlink(url, text):
    """Generate Google Sheets hyperlink formula"""
    return f'=HYPERLINK("{url}", "{text}")'

def get_existing_entries(sheet_name):
    """Fetch existing titles from Google Sheet"""
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.worksheet(sheet_name)
        return set(worksheet.col_values(1))
    except:
        return set()

def update_sheet(data, sheet_name):
    """Update Google Sheet with new entries"""
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = sheet.worksheet(sheet_name)
        except:
            worksheet = sheet.add_worksheet(title=sheet_name, rows=1000, cols=4)

        existing = get_existing_entries(sheet_name)
        new_entries = [row for row in data if row[0] not in existing]
        
        if new_entries:
            worksheet.append_rows(new_entries)
            print(f"✅ Added {len(new_entries)} to {sheet_name}")
        else:
            print(f"ℹ️ No new entries for {sheet_name}")
    except Exception as e:
        print(f"⚠️ Sheet error: {str(e)}")

# PRWeb Scraper
def scrape_prweb():
    """Scrape and process PRWeb articles"""
    session = setup_requests_session()
    session.headers['User-Agent'] = random.choice(USER_AGENTS)
    
    try:
        response = session.get("https://www.prweb.com/releases/news-releases-list/", 
                             timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.select('article.release, div.card, div.news-item')
        return process_prweb_articles(session, articles)
    except Exception as e:
        print(f"⚠️ PRWeb error: {str(e)}")
        return {'fintech': [], 'hrtech': []}

def process_prweb_articles(session, articles):
    """Process PRWeb articles"""
    results = {'fintech': [], 'hrtech': []}
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=MAX_ARTICLE_AGE_HOURS)
    
    for article in articles:
        try:
            # Extract article data
            title_link = article.select_one('h2 a, h3 a')
            if not title_link or not title_link.get('href'):
                continue
                
            url = title_link['href']
            if not url.startswith('http'):
                url = f'https://www.prweb.com{url}'
            
            date_element = article.select_one('time, .date')
            if not date_element:
                continue
                
            pub_date = parse_date(date_element.get('datetime', date_element.get_text()))
            if not pub_date or pub_date < cutoff:
                continue
            
            # Get content and categorize
            content = get_prweb_content(session, url)
            category = categorize_content(content)
            
            if category:
                entry = [
                    create_hyperlink(url, title_link.get_text(strip=True)),
                    pub_date.strftime('%Y-%m-%d %H:%M'),
                    category.upper(),
                    content[:4950]  # Truncate for Sheets cell limit
                ]
                results[category].append(entry)
                
            time.sleep(random.uniform(0.5, 1.5))
            
        except Exception as e:
            print(f"⚠️ PRWeb processing error: {str(e)}")
    
    return results

def get_prweb_content(session, url):
    """Extract PRWeb article content"""
    try:
        response = session.get(url, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        return ' '.join(p.get_text() for p in soup.select('.release-body p'))[:4950]
    except:
        return ""

# BusinessWire Scraper
def scrape_businesswire():
    """Scrape BusinessWire using requests with Selenium fallback"""
    try:
        response = requests.get("https://www.businesswire.com/portal/site/home/news/",
                              headers={'User-Agent': random.choice(USER_AGENTS)},
                              timeout=20)
        return process_businesswire(response.text)
    except:
        return process_businesswire_selenium()

def process_businesswire_selenium():
    """Selenium fallback for BusinessWire"""
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get("https://www.businesswire.com/portal/site/home/news/")
        time.sleep(5)
        html = driver.page_source
        driver.quit()
        return process_businesswire(html)
    except Exception as e:
        print(f"⚠️ BusinessWire error: {str(e)}")
        return {'fintech': [], 'hrtech': []}

def process_businesswire(html):
    """Process BusinessWire articles"""
    results = {'fintech': [], 'hrtech': []}
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=MAX_ARTICLE_AGE_HOURS)
    soup = BeautifulSoup(html, "html.parser")
    
    for article in soup.find_all("a", class_="bwTitleLink"):
        try:
            title = article.text.strip()
            url = f"https://www.businesswire.com{article['href']}"
            
            # Extract and parse date
            parent = article.find_parent()
            date_element = parent.find('span', class_='bwTimestamp') if parent else None
            pub_date = parse_businesswire_date(date_element)
            
            if not pub_date or pub_date < cutoff:
                continue
            
            # Categorize based on title (BusinessWire content requires paid API)
            category = categorize_content(title)
            
            if category:
                entry = [
                    create_hyperlink(url, title),
                    pub_date.strftime('%Y-%m-%d %H:%M'),
                    category.upper(),
                    title[:4950]  # Use title as content fallback
                ]
                results[category].append(entry)
                
        except Exception as e:
            print(f"⚠️ BusinessWire processing error: {str(e)}")
    
    return results

def parse_businesswire_date(element):
    """Parse BusinessWire date format"""
    if not element:
        return None
    try:
        return datetime.datetime.strptime(element.text.strip(), '%B %d, %Y').astimezone()
    except:
        return None

# Common Functions
def parse_date(date_str):
    """Flexible date parser"""
    formats = [
        '%Y-%m-%dT%H:%M:%S%z',
        '%b %d, %Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y %I:%M %p'
    ]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date_str, fmt).astimezone()
        except:
            continue
    return None

def categorize_content(text):
    """Categorize content using regex patterns"""
    text = text.lower()
    fintech = len(FINTECH_KEYWORDS.findall(text))
    hrtech = len(HRTECH_KEYWORDS.findall(text))
    
    if fintech > hrtech:
        return 'fintech'
    elif hrtech > fintech:
        return 'hrtech'
    return None

# Main Execution
def main_execution():
    """Main scraping workflow"""
    print("\n" + "="*50)
    print(f"🚀 Starting scrape at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        prweb_data = scrape_prweb()
        businesswire_data = scrape_businesswire()
        
        combined_data = {
            'fintech': prweb_data['fintech'] + businesswire_data['fintech'],
            'hrtech': prweb_data['hrtech'] + businesswire_data['hrtech']
        }
        
        update_sheet(combined_data['fintech'], "fintech news")
        update_sheet(combined_data['hrtech'], "hrtech news")
        
        print(f"✅ Completed at {datetime.datetime.now().strftime('%H:%M:%S')}")
        print("="*50)
        
    except Exception as e:
        print(f"⚠️ Critical error: {str(e)}")

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Schedule job every 30 minutes
    scheduler.add_job(
        main_execution,
        trigger=IntervalTrigger(minutes=30),
        max_instances=1,
        coalesce=True
    )
    
    print("🕒 Starting news aggregator scheduler")
    print("📅 Runs every 30 minutes. Press Ctrl+C to exit.")
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()
        print("👋 Scheduler stopped")