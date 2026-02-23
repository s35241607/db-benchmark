import time
import psycopg2
import pymysql
from clickhouse_driver import Client

# Database Connections
CONFIG_PG_DEFAULT = {"host": "localhost", "port": 5432, "user": "user", "password": "password", "dbname": "procurement"}
CONFIG_PG_OLAP = {"host": "localhost", "port": 5433, "user": "user", "password": "password", "dbname": "procurement"}
CONFIG_STARROCKS = {"host": "localhost", "port": 9030, "user": "root", "password": "", "database": "procurement"}
CONFIG_CLICKHOUSE = {"host": "localhost", "port": 9000, "user": "user", "password": "password", "database": "procurement"}

queries = {
    "Scenario 1: End-to-End Procurement Cycle Time & Supplier Performance": """
        SELECT 
            i.item_category,
            s.supplier_name,
            COUNT(DISTINCT pr.pr_id) as total_prs,
            COUNT(DISTINCT r.receipt_id) as total_receipts,
            AVG(r.received_qty) as avg_received_qty
        FROM fct_pr pr
        JOIN fct_pr_line prl ON pr.pr_id = prl.pr_id
        JOIN fct_rfq rfq ON pr.pr_id = rfq.pr_id
        JOIN fct_po po ON rfq.pr_id = po.po_id
        JOIN fct_po_line pol ON po.po_id = pol.po_id
        JOIN fct_receipt r ON pol.po_line_id = r.po_line_id
        JOIN dim_supplier s ON po.supplier_id = s.supplier_id
        JOIN dim_item i ON pol.item_id = i.item_id
        GROUP BY 
            i.item_category,
            s.supplier_name
        ORDER BY 
            total_prs DESC
        LIMIT 100;
    """,
    "Scenario 2: Outstanding Quantity & Delivery Risk Assessment": """
        WITH po_receipts AS (
            SELECT 
                po_line_id,
                SUM(received_qty) as total_received
            FROM fct_receipt
            GROUP BY po_line_id
        )
        SELECT 
            i.item_category,
            pol.po_id,
            pol.po_qty,
            COALESCE(pr.total_received, 0) as total_received,
            (pol.po_qty - COALESCE(pr.total_received, 0)) as outstanding_qty
        FROM fct_po_line pol
        JOIN dim_item i ON pol.item_id = i.item_id
        LEFT JOIN po_receipts pr ON pol.po_line_id = pr.po_line_id
        WHERE pol.po_qty > COALESCE(pr.total_received, 0)
        ORDER BY outstanding_qty DESC
        LIMIT 100;
    """,
    "Scenario 3: Material Spend & Purchase Price Variance (PPV)": """
        SELECT 
            item_category,
            item_code,
            total_spend,
            total_ppv,
            RANK() OVER (PARTITION BY item_category ORDER BY total_ppv DESC) as ppv_rank_in_category
        FROM (
            SELECT 
                i.item_category,
                i.item_code,
                SUM(pol.po_qty * pol.unit_price) as total_spend,
                SUM(pol.po_qty * (pol.unit_price - i.standard_cost)) as total_ppv
            FROM fct_po_line pol
            JOIN dim_item i ON pol.item_id = i.item_id
            GROUP BY 
                i.item_category,
                i.item_code,
                i.standard_cost
        ) sub
        ORDER BY total_ppv DESC
        LIMIT 100;
    """
}

def execute_pg(config, query):
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end = time.time()
    cursor.close()
    conn.close()
    return end - start

def execute_mysql(config, query):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end = time.time()
    cursor.close()
    conn.close()
    return end - start

def execute_clickhouse(config, query):
    client = Client(**config)
    start = time.time()
    results = client.execute(query)
    end = time.time()
    client.disconnect()
    return end - start

databases = {
    "PostgreSQL (Default)": (execute_pg, CONFIG_PG_DEFAULT),
    "PostgreSQL (OLAP Tuned)": (execute_pg, CONFIG_PG_OLAP),
    "ClickHouse": (execute_clickhouse, CONFIG_CLICKHOUSE),
    "StarRocks": (execute_mysql, CONFIG_STARROCKS),
}

def main():
    print("Starting DB Benchmark...")
    results = {db: {} for db in databases}
    
    # Warm up queries (fetch but dont measure) to put data in memory for fairness
    for name, query in queries.items():
        print(f"\\nRunning {name}...")
        for db_name, (func, config) in databases.items():
            try:
                # Warm-up run
                func(config, query)
                
                # Timed runs
                runs = 5
                time_taken = 0
                for _ in range(runs):
                    duration = func(config, query)
                    time_taken += duration
                avg_time = time_taken / runs
                results[db_name][name] = avg_time
                print(f"  [{db_name}] Avg Time: {avg_time:.4f}s")
            except Exception as e:
                print(f"  [{db_name}] ERROR: {e}")
                results[db_name][name] = "ERROR"

    # Write report
    with open("benchmark_report.md", "w") as f:
        f.write("# Database Performance Benchmark Report\\n\\n")
        f.write("## Scenarios Execution Time (Seconds)\\n\\n")
        
        # Header
        f.write("| Database | " + " | ".join(queries.keys()) + " |\\n")
        f.write("|----------|" + "|".join(["-"*len(q) for q in queries.keys()]) + "|\\n")
        
        for db_name in databases.keys():
            row = f"| {db_name} | "
            row += " | ".join([
                f"{results[db_name][q]:.4f}s" if isinstance(results[db_name][q], float) else str(results[db_name][q]) 
                for q in queries.keys()
            ])
            row += " |\\n"
            f.write(row)
            
    print("\\nBenchmark complete. Report saved to benchmark_report.md")

if __name__ == "__main__":
    main()
