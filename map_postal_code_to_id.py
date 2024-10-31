import pandas as pd

# Read both CSV files
pincodes_df = pd.read_csv('pincodes.csv')
data_df = pd.read_csv('output6.csv')

# Convert pincode column in pincodes_df to integer to match format
pincodes_df['pincode'] = pincodes_df['pincode'].astype(str).astype(int)

# Create a mapping dictionary from pincode to id
pincode_to_id = dict(zip(pincodes_df['pincode'], pincodes_df['id']))

# Add new column with mapped IDs
data_df['pincode_id'] = data_df['postcode'].map(pincode_to_id)

# Save the result
data_df.to_csv('output7.csv', index=False)

# Print sample to verify mapping (optional)
print(data_df[['postcode', 'pincode_id']].head())
