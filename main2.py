from db import Database
from Logger import Logger
from typing import Dict, List, Any
import requests
from bs4 import BeautifulSoup
import asyncio


class ParallelScraper:
    def __init__(self, batch_size: int = 10, timeout: int = 10, max_retries: int = 3):
        self.db = Database()
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries

    async def fetch_url_data(self, url: str) -> Dict[str, str]:
        """
        Fetch and parse data from the provided HTML
        """
        try:
            # Make the request and get the HTML
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract the address and image URL from the meta tags
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
        batch_size=10,
        timeout=10,
        max_retries=10
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
