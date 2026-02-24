import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_SUPPLIERS = 1000
NUM_ITEMS = 10000

print(f"Generating enriched dim_supplier.csv ({NUM_SUPPLIERS} rows)...")
with open('seeds/dim_supplier.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'supplier_id', 'supplier_name', 'supplier_group', 'vendor_type', 
        'country', 'city', 'payment_terms', 'currency', 'risk_score',
        'has_esg_report', 'last_audit_date', 'contract_status'
    ])
    
    countries = ['TW', 'US', 'JP', 'KR', 'DE', 'SG', 'CN', 'VN']
    vendor_types = ['Manufacturer', 'Distributor', 'Service Provider', 'OEM']
    payment_terms = ['NET30', 'NET60', 'NET90', 'COD', 'Prepaid']
    contract_statuses = ['Active', 'Expired', 'Negotiating', 'Blacklisted']
    
    for i in range(1, NUM_SUPPLIERS + 1):
        country = countries[i % len(countries)]
        vendor_type = vendor_types[i % len(vendor_types)]
        term = payment_terms[i % len(payment_terms)]
        status = contract_statuses[i % len(contract_statuses)]
        risk_score = round(random.uniform(1.0, 5.0), 2)
        has_esg = 1 if random.random() > 0.3 else 0
        audit_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        
        writer.writerow([
            i, f"Global Tech Solutions {i:04d}", "Strategic Partners", vendor_type,
            country, "Business Center", term, "USD", risk_score,
            has_esg, audit_date, status
        ])

print(f"Generating enriched dim_item.csv ({NUM_ITEMS} rows)...")
with open('seeds/dim_item.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'item_id', 'item_code', 'item_name', 'item_category', 'item_group',
        'tech_node', 'wafer_size', 'standard_cost', 'list_price', 
        'min_order_qty', 'lead_time_days', 'safety_stock', 'is_critical_material',
        'lifecycle_status', 'yield_rate_expected'
    ])
    
    categories = ['Wafer', 'Chemical', 'Gas', 'Equipment', 'Packaging', 'Substrate']
    nodes = ['3nm', '5nm', '7nm', '14nm', '28nm', 'Legacy']
    wafer_sizes = ['12-inch', '8-inch', '6-inch']
    statuses = ['Active', 'In-Development', 'Obsolete', 'EOL']
    
    for i in range(1, NUM_ITEMS + 1):
        cat = categories[i % len(categories)]
        node = nodes[i % len(nodes)]
        size = wafer_sizes[i % len(wafer_sizes)]
        status = statuses[i % len(statuses)]
        cost = round(100.0 + (i % 500) * 12.5, 2)
        is_critical = 1 if (i % 10 == 0) else 0
        yield_rate = round(random.uniform(0.85, 0.99), 4)
        
        writer.writerow([
            i, f"SMIC-{i:06d}", f"High Precision {cat} Component", cat, "Direct Material",
            node, size, cost, round(cost * 1.5, 2),
            50, 14 + (i % 45), 100, is_critical,
            status, yield_rate
        ])

print("Done generating enriched dimension seeds.")
