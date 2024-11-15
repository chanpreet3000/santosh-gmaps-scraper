import pandas as pd
from typing import Dict, Any, List
import asyncio
from db import Database
from tqdm import tqdm
from Logger import Logger


class InsertDataManager:
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.db = Database()

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single row of data into the format we want to store
        """
        return {
            'city_id': str(row['city_id']),
            'link': row['link'],
            'website': row['website'],
            'title': row['title'],
            'section_id': str(row['section_id']),
            'scraped': False,
            'images_scraped': False,
            'processing_status': 'pending',
            'retry_count': 0
        }

    async def get_existing_links(self, links: List[str]) -> set:
        """
        Get existing links from MongoDB
        """
        existing_records = await self.db.queue_collection.find(
            {'link': {'$in': links}},
            {'link': 1}
        ).to_list(None)
        return {record['link'] for record in existing_records}

    async def process_csv(self, input_file: str) -> None:
        """
        Process CSV file and upload unique data to MongoDB
        """
        try:
            # Connect to database
            await self.db.connect()

            # Read CSV file and drop duplicates within the CSV itself
            Logger.info('Reading CSV file...')
            df = pd.read_csv(
                input_file,
                dtype={
                    'city_id': str,
                    'link': str,
                    'website': str,
                    'title': str,
                    'section_id': str
                }
            )

            # Remove duplicates from DataFrame based on 'link'
            initial_count = len(df)
            df.drop_duplicates(subset=['link'], keep='first', inplace=True)
            duplicates_removed = initial_count - len(df)
            if duplicates_removed > 0:
                Logger.info(f"Removed {duplicates_removed} duplicate records from CSV")

            total_rows = len(df)
            total_batches = (total_rows + self.batch_size - 1) // self.batch_size

            Logger.info(f"Total unique rows to process: {total_rows}")
            Logger.info(f"Batch size: {self.batch_size}")
            Logger.info(f"Total batches: {total_batches}")

            # Process in batches
            processed = 0
            skipped = 0
            with tqdm(total=total_rows, desc="Uploading to MongoDB") as pbar:
                for batch_start in range(0, total_rows, self.batch_size):
                    batch_end = min(batch_start + self.batch_size, total_rows)
                    batch_df = df.iloc[batch_start:batch_end]

                    # Get all links in current batch
                    batch_links = batch_df['link'].tolist()

                    # Check which links already exist in MongoDB
                    existing_links = await self.get_existing_links(batch_links)

                    # Process only new records
                    batch_data = []
                    for _, row in batch_df.iterrows():
                        if row['link'] not in existing_links:
                            processed_row = self.process_row(row.to_dict())
                            batch_data.append(processed_row)
                        else:
                            skipped += 1

                    # Insert batch if there are new records
                    if batch_data:
                        try:
                            await self.db.queue_collection.insert_many(batch_data)
                            processed += len(batch_data)
                        except Exception as e:
                            Logger.error(f"Error inserting batch {batch_start // self.batch_size + 1}", e)
                            continue

                    # Update progress bar for both processed and skipped records
                    pbar.update(len(batch_df))

            Logger.info('Processing completed!')
            Logger.info(f'Successfully processed and uploaded {processed} new records')
            Logger.info(f'Skipped {skipped} existing records')

        except Exception as e:
            Logger.error(f"Error processing CSV file", e)
        finally:
            await self.db.close()


async def main():
    input_file = "input.csv"
    processor = InsertDataManager(batch_size=5000)
    await processor.process_csv(input_file)


if __name__ == "__main__":
    asyncio.run(main())
