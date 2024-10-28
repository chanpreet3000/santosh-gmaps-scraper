import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import csv
from tqdm import tqdm
import concurrent.futures
import time
from contextlib import contextmanager


class GoogleMapsScraper:
    def __init__(self, max_workers=4, max_retries=3):
        self.max_workers = max_workers
        self.max_retries = max_retries

    def get_chrome_options(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(
            f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36')
        return chrome_options

    @contextmanager
    def create_driver(self):
        driver = webdriver.Chrome(options=self.get_chrome_options())
        try:
            yield driver
        finally:
            driver.quit()

    def get_meta_data(self, driver):
        """Use JavaScript to extract meta tag data"""
        script = """
        return {
            address: (() => {
                const metaTags = document.getElementsByTagName('meta');
                for (let i = 0; i < metaTags.length; i++) {
                    const property = metaTags[i].getAttribute('property');
                    if (property === 'og:title') {
                        return metaTags[i].getAttribute('content');
                    }
                }
                return null;
            })(),
            image: (() => {
                const metaTags = document.getElementsByTagName('meta');
                for (let i = 0; i < metaTags.length; i++) {
                    const property = metaTags[i].getAttribute('property');
                    if (property === 'og:image') {
                        return metaTags[i].getAttribute('content');
                    }
                }
                return null;
            })()
        }
        """
        return driver.execute_script(script)

    def scrape_place_data(self, row):
        try:
            city_id = row['CityID']
            url = row['URL']
            unknown = row['Unknown']
            place_name = row['PlaceName']
            category_id = row['CategoryID']

            if pd.isna(url) or not isinstance(url, str):
                return [city_id, url, unknown, place_name, category_id, "", ""]

            with self.create_driver() as driver:
                for attempt in range(self.max_retries):
                    driver.get(url)
                    time.sleep(2)  # Initial wait for page load

                    # Get meta data using JavaScript
                    meta_data = self.get_meta_data(driver)
                    address = meta_data.get('address', '')
                    image_url = meta_data.get('image', '')

                    # If either value is null/empty, refresh and try again
                    if not address or not image_url:
                        print(f"Attempt {attempt + 1}: Missing data, retrying...")
                        driver.refresh()
                        time.sleep(2)  # Wait after refresh
                        continue
                    else:
                        print(f"Attempt {attempt + 1}: Data retrieved successfully", address, image_url)
                        break

                # Log if we still don't have data after all retries
                if not address or not image_url:
                    print(f"Failed to get complete data for {url} after {self.max_retries} attempts")

                return [city_id, url, unknown, place_name, category_id, address, image_url]

        except Exception as e:
            print(f"Error scraping data from {url}: {e}")
            return [city_id, url, unknown, place_name, category_id, "", ""]

    def process_excel(self, input_file, output_file):
        try:
            # Read Excel file
            df = pd.read_excel(input_file, names=['CityID', 'URL', 'Unknown', 'PlaceName', 'CategoryID'])

            all_results = []

            # Process all rows in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_row = {executor.submit(self.scrape_place_data, row): row
                                 for row in df.to_dict('records')}

                # Collect results with progress bar
                for future in tqdm(concurrent.futures.as_completed(future_to_row),
                                   total=len(future_to_row),
                                   desc="Scraping places"):
                    result = future.result()
                    all_results.append(result)

            # Write all results to CSV at once
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['City ID', 'URL', 'Unknown', 'Place Name', 'Category ID', 'Address', 'Image URL'])
                writer.writerows(all_results)

        except Exception as e:
            print(f"Error processing Excel file: {e}")


def main():
    scraper = GoogleMapsScraper(max_workers=10, max_retries=3)
    input_file = 'input.xlsx'
    output_file = 'output.csv'

    print(f"Starting to process {input_file}")
    scraper.process_excel(input_file, output_file)
    print(f"Processing complete. Results saved to {output_file}")


if __name__ == "__main__":
    main()
