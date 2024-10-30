import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to MongoDB
client = MongoClient(os.getenv('MONGO_URI'))
db = client["santosh-gmaps"]
collection = db["queue"]

# Fetch data from the collection
data = list(collection.find({}))

# Create a DataFrame and save it as CSV
df = pd.DataFrame(data)
df.to_csv("output2.csv", index=False)
