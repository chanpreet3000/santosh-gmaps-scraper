import pandas as pd

# Read the CSV file
df = pd.read_csv('output5.csv')

# Convert postcode to integer type
df['postcode'] = df['postcode'].fillna(0).astype(int)
# Remove rows where postcode is 0 (was originally null)
df_clean = df[df['postcode'] != 0]

# Save the filtered data
df_clean.to_csv('output6.csv', index=False)

# Print sample to verify format (optional)
print(df_clean['postcode'].head())