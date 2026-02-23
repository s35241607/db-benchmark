import csv
from datetime import datetime, timedelta

print("Generating dim_date_hour.csv...")
start_date = datetime(2024, 1, 1)
with open('seeds/dim_date_hour.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['date_hour_id', 'datetime_val', 'date_val', 'year_val', 'month_val', 'day_val', 'hour_val', 'is_working_day', 'is_working_hour'])
    for i in range(43824):
        dt = start_date + timedelta(hours=i)
        is_working_day = 1 if dt.weekday() < 5 else 0
        is_working_hour = 1 if 9 <= dt.hour < 18 else 0
        writer.writerow([i+1, dt.strftime('%Y-%m-%d %H:%M:%S'), dt.strftime('%Y-%m-%d'), dt.year, dt.month, dt.day, dt.hour, is_working_day, is_working_hour])

print("Generating dim_supplier.csv...")
with open('seeds/dim_supplier.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['supplier_id', 'supplier_name', 'country', 'rating'])
    countries = ['TW', 'US', 'JP', 'KR', 'DE']
    for i in range(1, 1001):
        country = countries[i % 5]
        rating = 1 + (i % 5) # 1 to 5
        writer.writerow([i, f"Supplier_{i:04d}", country, rating])

print("Generating dim_item.csv...")
with open('seeds/dim_item.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['item_id', 'item_code', 'item_category', 'standard_cost', 'lead_time_days'])
    categories = ['Wafer', 'Chemical', 'Gas', 'Equipment', 'Packaging']
    for i in range(1, 10001):
        category = categories[i % 5]
        cost = 10.0 + (i % 100) * 5.5
        lead_time = 7 + (i % 30) # 7 to 36 days
        writer.writerow([i, f"ITEM_{i:06d}", category, round(cost, 2), lead_time])

print("Done generating dimension seeds.")
