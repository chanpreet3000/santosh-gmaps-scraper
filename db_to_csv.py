import pandas as pd
import asyncio

from db import Database


async def main():
    db = Database()
    await db.connect()
    data = list

    # Fetch data from the collection
    data = list(db.queue_collection.find({}))

    # Create a DataFrame and save it as CSV
    df = pd.DataFrame(data)
    df.to_csv("output.csv", index=False)


if __name__ == "__main__":
    asyncio.run(main())
