import csv
import random
from datetime import datetime, timedelta

NUM_ROWS = 120000
NUM_COLS = 100

print(f"Generating dims with {NUM_ROWS} rows and {NUM_COLS} columns...")
with open('seeds/dim_item_wide.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    # Generate exactly 100 columns: 4 fixed + 96 metrics
    headers = ['item_id', 'item_code', 'category', 'status']
    for i in range(1, 97):
        headers.append(f"metric_{i}")
    writer.writerow(headers)
    
    cats = ['Wafer', 'Chemical', 'Electronic', 'Mechanical', 'RawMaterial']
    statuses = ['Active', 'Obsolete', 'Draft', 'Review']
    
    for i in range(1, NUM_ROWS + 1):
        row = [
            i, 
            f"ITEM-{i:08d}", 
            cats[i % len(cats)], 
            statuses[i % len(statuses)]
        ]
        # Add 96 random metric columns
        for j in range(1, 97):
            row.append(round(random.uniform(0.1, 1000.0), 2))
        writer.writerow(row)

print("Done generating 100-column wide seed data.")
