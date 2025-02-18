# Automated News Aggregator 📰

A Python-based solution that scrapes and categorizes FinTech/HRTech news from PRWeb and BusinessWire, then updates Google Sheets in real-time.

## Features
- Dual-source scraping (PRWeb + BusinessWire)
- 24-hour news filtering
- Google Sheets integration
- Headless browser fallback
- Automated duplicate prevention
- Error handling & retry logic
- Automation Every 30 minutes

## Architecture
```mermaid
graph TD
    A[Scraper Scheduler] --> B(PRWeb)
    A --> C(BusinessWire)
    B --> D[News Processing]
    C --> D
    D --> E[Google Sheets Update]
