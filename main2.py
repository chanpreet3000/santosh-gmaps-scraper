from db import Database
from Logger import Logger
from typing import Dict, List, Any
import aiohttp
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime


class ParallelScraper:
    def __init__(self, batch_size: int = 10, timeout: int = 10, max_retries: int = 3):
        self.db = Database()
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.proxies = self.load_proxies_from_file('proxies.txt')
        self.current_proxy_index = 0
        self.session = None
        self.total_processed = 0
        Logger.info(f"Initialized scraper with {len(self.proxies)} proxies")

    def load_proxies_from_file(self, file_name: str) -> list[str]:
        proxies = []
        with open(file_name, 'r') as file:
            for line in file:
                parts = line.strip().split(':')
                if len(parts) == 4:
                    proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                    proxies.append(proxy_url)
        return proxies

    async def fetch_url_data(self, url: str) -> Dict[str, str]:
        """
        Fetch and parse data from the provided URL using aiohttp
        """
        try:
            proxy_url = self.proxies[self.current_proxy_index]
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)

            async with self.session.get(
                    url,
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                    }
            ) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                address = soup.select_one('meta[property="og:title"]').get('content')
                image_url = soup.select_one('meta[property="og:image"]').get('content')

                return {
                    'address': address,
                    'image_url': image_url,
                }

        except Exception as e:
            raise Exception(f"Error extracting data from HTML: {str(e)}")

    async def process_record(self, record: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Process a single record
        """
        try:
            url = record['link']
            record_id = record['_id']

            scraped_data = await self.fetch_url_data(url)

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

            Logger.info(f"Successfully processed record {record_id}")
            self.total_processed += 1
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

    async def process_batch(self, batch: List[Dict[str, Any]]):
        """
        Process a batch of records concurrently using asyncio.gather
        """
        tasks = [
            self.process_record(record)
            for record in batch if record['retry_count'] < self.max_retries
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        # Log batch statistics
        Logger.info(
            f"Batch Statistics",
            {
                'batch_size': len(batch),
                'total_processed': self.total_processed,
                'total_processed_so_far': self.total_processed,
                'elapsed_time': (datetime.now() - self.start_time).total_seconds()
            }
        )

    async def run(self):
        """
        Main method to run the scraper
        """
        try:
            # Connect to database
            await self.db.connect()

            # Initialize aiohttp session
            self.session = aiohttp.ClientSession()
            self.start_time = datetime.now()

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
            batch_number = 0
            while True:
                batch_number += 1

                # Get batch of unscraped records
                batch = await self.db.queue_collection.find({
                    'scraped': False,
                    'retry_count': {'$lt': self.max_retries}
                }).limit(self.batch_size).to_list(length=self.batch_size)

                if not batch:
                    break

                Logger.debug(f"Starting batch {batch_number}")
                await self.process_batch(batch)

                # Calculate and log progress
                progress = (self.total_processed / total_unscraped) * 100

                Logger.info(
                    "Progress Update",
                    {
                        'batch_number': batch_number,
                        'progress_percentage': f"{progress:.2f}%",
                        'records_processed': self.total_processed,
                        'total_records': total_unscraped,
                    }
                )

            processed = await self.db.queue_collection.count_documents({
                'scraped': True
            })

            Logger.info(f"Scraping Completed {processed} records.")

        except Exception as e:
            Logger.error("Error in scraping process", e)
        finally:
            if self.session:
                await self.session.close()
            await self.db.close()


async def main():
    scraper = ParallelScraper(
        batch_size=10,
        timeout=10,
        max_retries=5
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
