import asyncio

from contextlib import contextmanager
from db import Database
from Logger import Logger
from typing import Dict, List, Any
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class ParallelScraper:
    def __init__(self, batch_size: int = 10, timeout: int = 10, max_retries: int = 3, delay_between_batches: int = 60):
        self.db = Database()
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.delay_between_batches = delay_between_batches
        self.proxies = self.load_proxies_from_file('proxies.txt')
        Logger.info(f"Loaded {len(self.proxies)} proxies")
        self.current_proxy_index = 0

    def load_proxies_from_file(self, file_name: str) -> list[str]:
        proxies = []
        with open(file_name, 'r') as file:
            for line in file:
                parts = line.strip().split(':')
                if len(parts) == 4:
                    proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                    proxies.append(proxy_url)
        return proxies

    @contextmanager
    def create_driver(self):
        from selenium_authenticated_proxy import SeleniumAuthenticatedProxy

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(
            f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36')

        # Get the proxy for this session
        proxy_url = self.proxies[self.current_proxy_index]

        # Initialize SeleniumAuthenticatedProxy
        proxy_helper = SeleniumAuthenticatedProxy(proxy_url=proxy_url)

        # Enrich Chrome options with proxy authentication
        proxy_helper.enrich_chrome_options(chrome_options)

        # Create the driver with enriched options
        driver = webdriver.Chrome(options=chrome_options)

        # Update the proxy index for round-robin rotation
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)

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
            image_url: (() => {
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

    async def process_record(self, record: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Process a single record
        """
        try:
            url = record['link']
            record_id = record['_id']

            Logger.debug(f"Fetching data from URL: {url}")
            with self.create_driver() as driver:
                driver.get(url)
                scraped_data = self.get_meta_data(driver)

            address = scraped_data['address']
            if (address is None or address == 'Google Maps'):
                raise Exception('Most likely a captcha page')

            # Update record
            update_data = {
                'scraped': True,
                'processing_status': 'processed',
                'address': scraped_data['address'],
                'image_url': scraped_data['image_url'],
            }

            await self.db.queue_collection.update_one(
                {'_id': record_id},
                {
                    '$set': update_data,
                    '$inc': {'retry_count': 1}
                }
            )

            Logger.info(f"Successfully processed record {record_id}", update_data)
            return record_id

        except Exception as e:
            await self.db.queue_collection.update_one(
                {'_id': record['_id']},
                {
                    '$set': {
                        'processing_status': 'failed',
                    },
                    '$inc': {'retry_count': 1}
                }
            )

            Logger.error(f"Error processing record {record['_id']}", e)
            return None

    async def process_batch(self, batch: List[Dict[str, Any]]) -> List[str]:
        """
        Process a batch of records concurrently using asyncio.gather
        """
        tasks = [
            self.process_record(record)
            for record in batch if record['retry_count'] < self.max_retries
        ]

        processed_ids = await asyncio.gather(*tasks)
        return [record_id for record_id in processed_ids if record_id is not None]

    async def run(self):
        """
        Main method to run the scraper
        """
        try:
            # Connect to database
            await self.db.connect()

            Logger.info("Starting parallel scraping process")

            # Get total count of unscraped records
            total_unscraped = await self.db.queue_collection.count_documents({
                'scraped': False,
                'retry_count': {'$lt': self.max_retries}
            })

            if total_unscraped == 0:
                Logger.info("No unscraped records found")
                return

            Logger.info(f"Found {total_unscraped} unscraped records")

            # Process in batches
            processed_count = 0
            while True:
                # Get batch of unscraped records
                batch = await self.db.queue_collection.find({
                    'scraped': False,
                    'retry_count': {'$lt': self.max_retries}
                }).limit(self.batch_size).to_list(length=self.batch_size)

                if not batch:
                    break

                # Process batch
                processed_ids = await self.process_batch(batch)
                processed_count += len(processed_ids)

                # Add delay between batches
                Logger.info(
                    f"Processed {processed_count} records so far. Delaying for {self.delay_between_batches} seconds before the next batch.")
                await asyncio.sleep(self.delay_between_batches)

            Logger.info(f"Scraping completed! Processed {processed_count} records successfully")

            # Log final statistics
            failed_count = await self.db.queue_collection.count_documents({
                'processing_status': 'failed'
            })
            retry_exceeded = await self.db.queue_collection.count_documents({
                'retry_count': {'$gte': self.max_retries}
            })

            Logger.info(f"Failed records: {failed_count}")
            Logger.info(f"Retry limit exceeded: {retry_exceeded}")

        except Exception as e:
            Logger.error("Error in scraping process", e)
        finally:
            await self.db.close()


async def main():
    scraper = ParallelScraper(
        batch_size=20,
        timeout=15,
        max_retries=10,
        delay_between_batches=10
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
