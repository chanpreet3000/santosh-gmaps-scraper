from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
from datetime import datetime
from Logger import Logger  # Import Logger

load_dotenv()


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.client = None
        self.db = None
        self.queue_collection = None
        self._initialized = True

    async def connect(self):
        """Connect to MongoDB database"""
        if not self.client:
            try:
                self.client = AsyncIOMotorClient(os.getenv('MONGO_URI'), serverSelectionTimeoutMS=10000)
                await self.client.server_info()  # Verify connection
                self.db = self.client['santosh-gmaps']
                self.queue_collection = self.db['queue']

                # Create index on scraped field for efficient querying
                await self.queue_collection.create_index('scraped')

                Logger.info("Successfully connected to MongoDB")
            except Exception as e:
                Logger.error(f"Failed to connect to MongoDB: {str(e)}")

    async def insert_queue_item(self, item: Dict[str, Any]) -> str:
        # Add metadata
        item['scraped'] = False
        item['created_at'] = datetime.utcnow()
        item['updated_at'] = datetime.utcnow()

        result = await self.queue_collection.insert_one(item)
        Logger.info(f"Inserted item with ID: {str(result.inserted_id)}")
        return str(result.inserted_id)

    async def get_unscraped_items(self) -> List[Dict[str, Any]]:
        cursor = self.queue_collection.find({'scraped': False})
        items = await cursor.to_list(length=None)
        return items

    async def mark_as_scraped(self, item_id: str) -> bool:
        from bson.objectid import ObjectId

        result = await self.queue_collection.update_one(
            {'_id': ObjectId(item_id)},
            {
                '$set': {
                    'scraped': True,
                    'updated_at': datetime.utcnow()
                }
            }
        )

        return result.modified_count > 0

    async def close(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self.queue_collection = None
            Logger.info("Closed MongoDB connection")
