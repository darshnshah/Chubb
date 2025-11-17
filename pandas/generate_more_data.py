import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Load existing data
existing_df = pd.read_csv('sample.csv')
print(f"Existing records: {len(existing_df)}")

# Get the max transaction_id
max_id = existing_df['transaction_id'].max()
print(f"Max transaction ID: {max_id}")

# Configuration
num_new_records = 990
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 12, 31)

# Data options
stores = [
    (1, 'Downtown'),
    (2, 'Suburb'),
    (3, 'Airport'),
    (4, 'Mall'),
    (5, 'University')
]

products = [
    (501, 'Notebook', 3.50),
    (502, 'Pen', 0.80),
    (503, 'Stapler', 5.25),
    (504, 'Folder', 1.20),
    (505, 'Marker', 1.75),
    (506, 'Highlighter', 0.90),
    (507, 'Scissors', 4.50),
    (508, 'Tape', 2.25),
    (509, 'Glue', 1.80),
    (510, 'Eraser', 0.50)
]

promo_codes = ['DISC10', 'DISC5', 'SAVE20', 'WELCOME', None, None, None]  # More None for missing values

# Generate new data
new_data = []
for i in range(max_id + 1, max_id + 1 + num_new_records):
    # Random date
    days_diff = (end_date - start_date).days
    random_days = random.randint(0, days_diff)
    transaction_date = start_date + timedelta(days=random_days)
    
    # Random store
    store_id, store_name = random.choice(stores)
    
    # Random product
    product_id, product_name, unit_price = random.choice(products)
    
    # Random quantity (1-10)
    quantity = random.randint(1, 10)
    
    # Calculate total
    total = round(quantity * unit_price, 2)
    
    # Random customer (some missing - about 5%)
    customer_id = random.randint(2001, 2100) if random.random() > 0.05 else ''
    
    # Random promo code (mostly missing - about 70%)
    promo_code = random.choice(promo_codes)
    
    new_data.append({
        'transaction_id': i,
        'store_id': store_id,
        'store_name': store_name,
        'date': transaction_date.strftime('%Y-%m-%d'),
        'product_id': product_id,
        'product_name': product_name,
        'quantity': quantity,
        'unit_price': unit_price,
        'total': total,
        'customer_id': customer_id,
        'promo_code': promo_code if promo_code else ''
    })

# Create DataFrame for new records
new_df = pd.DataFrame(new_data)

# Add some duplicates (about 1%)
num_duplicates = int(num_new_records * 0.01)
duplicate_indices = random.sample(range(len(new_df)), num_duplicates)
duplicates = new_df.iloc[duplicate_indices].copy()
new_df = pd.concat([new_df, duplicates], ignore_index=True)

# Combine with existing data
combined_df = pd.concat([existing_df, new_df], ignore_index=True)

# Shuffle the entire dataset
combined_df = combined_df.sample(frac=1, random_state=42).reset_index(drop=True)

# Save to CSV
combined_df.to_csv('sample.csv', index=False)

print(f"\nAdded {len(new_df)} new records")
print(f"Total records now: {len(combined_df)}")
print(f"\nFirst few records of combined data:")
print(combined_df.head(10))
print(f"\nLast few records of combined data:")
print(combined_df.tail(10))
print(f"\nData shape: {combined_df.shape}")
print(f"\nMissing values:")
print(combined_df.isnull().sum())
print(f"\nTransaction ID range: {combined_df['transaction_id'].min()} to {combined_df['transaction_id'].max()}")
