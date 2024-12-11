import json
import asyncio
from tqdm import tqdm
from datetime import datetime
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup
from pathlib import Path
from aiohttp import ClientSession

Firecrawl = FirecrawlApp(api_key="fc-6c67c44de7ec4f068589ee72a7ba66d5")

# Load data from JSON file
def load_data(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

# Save processed data to JSON file
def save_data(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

# Check URL status asynchronously
async def check_url_status(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                return 'OK'
            elif response.status == 404:
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')
                script_tags = soup.find_all('script')
                if any(keyword in text.lower() for keyword in ['not found', '404', 'page not found']):
                    return '404'
                elif len(script_tags) > 0:
                    return '404_Dynamic'
                else:
                    return '404'
            elif response.status == 500:
                return '500'
            else:
                return 'FAILED'
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return 'ERROR'

# Scrape URL using Firecrawl asynchronously
async def scrape_url(url):
    try:
        scrape_response = Firecrawl.async_batch_scrape_urls(
            [url],  # Pass the URL as a list to match expected type
            {
                'formats': ['html', 'markdown'],
                'waitFor': 3000,
                'timeout': 15000
            }
        )
        return scrape_response
    except Exception as e:
        print(f"Error scraping URL {url} with Firecrawl: {e}")
        return None

# Process URLs asynchronously
async def process_urls(data):
    async with ClientSession() as session:
        for idx, item in enumerate(tqdm(data, desc="Processing URLs")):
            url = item.get('DocumentIdentifier')
            if not url:
                continue

            # Step 3: Check if URL is OK, 404, 500, or extraction failed
            item['URL_Status'] = await check_url_status(session, url)

            # Step 4: Scrape HTML using Firecrawl
            if item['URL_Status'] == 'OK' or item['URL_Status'] == '404_Dynamic':
                scrape_response = await scrape_url(url)
                if scrape_response and 'id' in scrape_response:
                    item['Scrape_ID'] = scrape_response['id']

            print(f"Processed {idx + 1}/{total_urls} URLs")

            # Save progress every 10 URLs or if all are processed
            if (idx + 1) % 10 == 0 or (idx + 1) == total_urls:
                save_data(data, f"processed_urls_{datetime.now().strftime('%y%m%d%H%M')}.json")

# Load data
data = load_data('env_biofuel_dataset_transformed.json')

# Add new columns for URL_Status and Scrape_ID
for item in data:
    item['URL_Status'] = None
    item['Scrape_ID'] = None

# Progress bar for URLs
total_urls = len(data)

# Run the processing loop
asyncio.run(process_urls(data))

# Final save
save_data(data, f"processed_urls_{datetime.now().strftime('%y%m%d%H%M')}.json")
