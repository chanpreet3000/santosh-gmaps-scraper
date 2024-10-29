import pandas as pd
from typing import Dict, Any
import asyncio
from db import Database
from tqdm import tqdm
from Logger import Logger


class CSVProcessor:
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
            'processing_status': 'pending',
            'retry_count': 0
        }

    async def process_csv(self, input_file: str) -> None:
        """
        Process CSV file and upload data to MongoDB
        """
        try:
            # Connect to database
            await self.db.connect()

            # Read CSV file
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

            total_rows = len(df)
            total_batches = (total_rows + self.batch_size - 1) // self.batch_size

            Logger.info(f"Total rows to process: {total_rows}")
            Logger.info(f"Batch size: {self.batch_size}")
            Logger.info(f"Total batches: {total_batches}")

            # Process in batches
            processed = 0
            with tqdm(total=total_rows, desc="Uploading to MongoDB") as pbar:
                for batch_start in range(0, total_rows, self.batch_size):
                    batch_end = min(batch_start + self.batch_size, total_rows)
                    batch_df = df.iloc[batch_start:batch_end]

                    # Process batch rows
                    batch_data = []
                    for _, row in batch_df.iterrows():
                        processed_row = self.process_row(row.to_dict())
                        batch_data.append(processed_row)

                    # Insert batch
                    try:
                        await self.db.queue_collection.insert_many(batch_data)
                        processed += len(batch_data)
                        pbar.update(len(batch_data))
                    except Exception as e:
                        Logger.error(f"Error inserting batch {batch_start // self.batch_size + 1}", e)
                        continue

            Logger.info(f'Processing completed!')
            Logger.info(f'Successfully processed and uploaded {processed} rows')

        except Exception as e:
            Logger.error(f"Error processing CSV file", e)
        finally:
            await self.db.close()


async def main():
    input_file = "input.csv"
    processor = CSVProcessor(batch_size=1000)
    await processor.process_csv(input_file)


if __name__ == "__main__":
    asyncio.run(main())
