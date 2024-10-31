import pandas as pd

df = pd.read_csv('output3.csv')


def process_address(address):
    if pd.isna(address):
        return address
    parts = str(address).split('·')
    return parts[1].strip() if len(parts) > 1 else address.strip()


address_columns = [col for col in df.columns if 'address' in col.lower()]
for col in address_columns:
    df[col] = df[col].apply(process_address)

df.to_csv('output4.csv', index=False)
