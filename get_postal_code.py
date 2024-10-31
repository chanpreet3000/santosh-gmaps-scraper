import pandas as pd
import re

# Read the CSV file
df = pd.read_csv('output4.csv')


# Function to extract Indian postcode (6 digits)
def extract_postcode(address):
    if pd.isna(address):
        return None
    match = re.search(r'\b\d{6}\b', str(address))
    return match.group() if match else None


# Extract postcode from address column
df['postcode'] = df['address'].apply(extract_postcode)

# Save to new CSV
df.to_csv('output5.csv', index=False)
