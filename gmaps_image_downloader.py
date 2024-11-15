import time
import os
import uuid
import aiohttp
import asyncio
from typing import Dict, List, Any
from Logger import Logger
from db import Database


class ParallelImageDownloader:
    def __init__(self, batch_size: int = 10, timeout: int = 10, max_retries: int = 3, batch_delay: int = 5):
        self.db = Database()
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.batch_delay = batch_delay
        self.session = None
        self.total_processed = 0
        self.images_dir = 'images'

        # Create images directory if it doesn't exist
        os.makedirs(self.images_dir, exist_ok=True)
        Logger.info(f"Initialized image downloader")

    async def download_image(self, image_url: str) -> str:
        """
        Download image from URL and save it with a unique name
        Returns the saved image filename
        """
        try:
            async with self.session.get(
                    image_url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                    }
            ) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")

                # Generate unique filename using UUID
                file_extension = image_url.split('.')[-1].split('?')[0]  # Handle URLs with query parameters
                if not file_extension or len(file_extension) > 4:
                    file_extension = 'jpg'  # Default to jpg if no valid extension found

                filename = f"{uuid.uuid4()}.{file_extension}"
                filepath = os.path.join(self.images_dir, filename)

                # Save image
                content = await response.read()
                with open(filepath, 'wb') as f:
                    f.write(content)

                return filename

        except Exception as e:
            raise Exception(f"Error downloading image: {str(e)}")

    async def process_record(self, record: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Process a single record
        """
        try:
            record_id = record['_id']
            image_url = record['image_url']

            # Download image
            image_filename = await self.download_image(image_url)

            # Update record
            update_data = {
                'images_scraped': True,
                'image_filename': image_filename,
                'processing_status': 'processed'
            }

            await self.db.queue_collection.update_one(
                {'_id': record_id},
                {
                    '$set': update_data,
                    '$inc': {'retry_count': 1}
                }
            )

            Logger.info(f"Successfully downloaded image for record {record_id}")
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

    async def run(self):
        """
        Main method to run the image downloader
        """
        try:
            # Connect to database
            await self.db.connect()

            # Initialize aiohttp session
            self.session = aiohttp.ClientSession()

            Logger.info("Starting parallel image downloading process")

            # Get total count of unprocessed images
            total_unprocessed = await self.db.queue_collection.count_documents({
                'images_scraped': False,
                'scraped': True,  # Only process records that have been scraped
                'retry_count': {'$lt': self.max_retries}
            })

            if total_unprocessed == 0:
                Logger.info("No unprocessed images found")
                return

            Logger.info(f"Found {total_unprocessed} unprocessed images")

            # Process in batches
            batch_number = 0
            while True:
                batch_number += 1

                # Get batch of unprocessed records
                batch = await self.db.queue_collection.find({
                    'images_scraped': False,
                    'scraped': True,
                    'retry_count': {'$lt': self.max_retries}
                }).limit(self.batch_size).to_list(length=self.batch_size)

                if not batch:
                    break

                Logger.debug(f"Starting batch {batch_number}")
                await self.process_batch(batch)

                # Calculate and log progress
                progress = (self.total_processed / total_unprocessed) * 100

                Logger.info(
                    "Progress Update",
                    {
                        'batch_number': batch_number,
                        'progress_percentage': f"{progress:.2f}%",
                        'records_processed': self.total_processed,
                        'total_records': total_unprocessed,
                    }
                )
                Logger.info(f'Sleeping for {self.batch_delay} seconds')
                time.sleep(self.batch_delay)

            processed = await self.db.queue_collection.count_documents({
                'images_scraped': True
            })

            Logger.info(f"Image downloading completed. {processed} images downloaded.")

        except Exception as e:
            Logger.error("Error in image downloading process", e)
        finally:
            if self.session:
                await self.session.close()
            await self.db.close()


async def main():
    downloader = ParallelImageDownloader(
        batch_size=100,
        timeout=15,
        max_retries=8,
        batch_delay=0,
    )
    await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())
