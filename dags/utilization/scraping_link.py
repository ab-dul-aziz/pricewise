import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import sleep

# ================== Constants ==================
BASE_URL = "https://www.rumah123.com/jual/cari/?q=rumah+jabodetabek&page={}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
OUTPUT_FILE = '/opt/airflow/data/link_properties.csv'

# ================== Helper Functions ==================

def fetch_page_html(url):
    """Fetch the HTML content of a given page URL."""
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to fetch {url}. Status code: {response.status_code}")
        return None

def parse_property_links(page_html):
    """Extract property links and titles from the page HTML."""
    property_links = []
    soup = BeautifulSoup(page_html, 'html.parser')

    # Loop through each property card
    for property_item in soup.find_all('div', {'class': 'card-featured__middle-section'}):
        link_tag = property_item.find('a', {'title': True})
        if link_tag:
            property_info = {
                'property_title': link_tag.get('title'),
                'property_url': link_tag.get('href')
            }
            property_links.append(property_info)

    return property_links

def save_to_csv(data, output_file):
    """Save the list of property links to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    print(f"Data successfully saved to {output_file}")

# ================== Main Scraping Function ==================

def ScrapingLink(start_page=1, end_page=2, sleep_time=2):
    """Main function to scrape property links from multiple pages."""
    print("Scraping process started...")
    all_property_links = []

    # Loop through pages
    for current_page in range(start_page, end_page + 1):
        page_url = BASE_URL.format(current_page)
        print(f"Processing page {current_page}...")

        # Fetch page HTML
        page_html = fetch_page_html(page_url)
        if page_html:
            # Parse property links from page
            property_links = parse_property_links(page_html)
            all_property_links.extend(property_links)
            print(f"Page {current_page}: {len(property_links)} properties found.")
        else:
            print(f"Skipping page {current_page} due to fetch failure.")

        # Sleep to avoid overloading the server
        sleep(sleep_time)

    # Save to CSV
    if all_property_links:
        save_to_csv(all_property_links, OUTPUT_FILE)
    else:
        print("No property data was collected.")

    print("Scraping process completed successfully.")

# ================== Run as Script ==================

if __name__ == "__main__":
    # Scrape property links from page 1 to 70
    ScrapingLink()
