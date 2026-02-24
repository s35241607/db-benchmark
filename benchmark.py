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
    "Scenario 1: Complex 11-Table Join for Semiconductor Supply Chain Yield Risk": """
        SELECT COUNT(*), MAX(CAST(supplier_name AS CHAR(255)))
        FROM (
            SELECT 
                i.tech_node,
                i.wafer_size,
                i.item_category,
                s.supplier_name,
                s.country,
                s.risk_score,
                COUNT(DISTINCT pr.pr_id) as total_prs,
                SUM(prl.pr_qty) as total_requested_qty,
                COUNT(DISTINCT r.receipt_id) as total_receipts,
                r.inspection_status,
                SUM(r.received_qty) as total_received_qty,
                SUM(r.rejected_qty) as total_rejected_qty,
                SUM(r.rejected_qty) / NULLIF(SUM(r.received_qty), 0) as scrap_rate
            FROM fct_pr pr
            JOIN fct_pr_line prl ON pr.pr_id = prl.pr_id
            JOIN fct_rfq rfq ON pr.pr_id = rfq.pr_id
            JOIN fct_po po ON rfq.pr_id = po.po_id
            JOIN fct_po_line pol ON po.po_id = pol.po_id
            JOIN fct_receipt r ON pol.po_line_id = r.po_line_id
            JOIN dim_supplier s ON po.supplier_id = s.supplier_id
            JOIN dim_item i ON pol.item_id = i.item_id
            WHERE i.is_critical_material = 1
              AND s.risk_score > 3.0
            GROUP BY 
                i.tech_node,
                i.wafer_size,
                i.item_category,
                s.supplier_name,
                s.country,
                s.risk_score,
                r.inspection_status
        ) sub;
    """,
    "Scenario 2: Advanced CTE Array-Like Supply vs Payment Terms Analysis": """
        SELECT COUNT(*), MAX(CAST(total_expected_value AS CHAR(255)))
        FROM (
            WITH supplier_metrics AS (
                SELECT 
                    supplier_id,
                    payment_terms,
                    has_esg_report,
                    vendor_type
                FROM dim_supplier
            ),
            po_receipts AS (
                SELECT 
                    po_line_id,
                    SUM(received_qty) as total_received,
                    SUM(accepted_qty) as total_accepted,
                    SUM(rejected_qty) as total_rejected
                FROM fct_receipt
                WHERE inspection_status != 'Pending'
                GROUP BY po_line_id
            )
            SELECT 
                i.item_category,
                i.item_group,
                sm.payment_terms,
                sm.vendor_type,
                pol.po_id,
                pol.po_qty,
                pol.unit_price,
                (pol.po_qty * pol.unit_price * (1 - pol.discount_pct / 100)) as total_expected_value,
                COALESCE(pr.total_received, 0) as total_received,
                COALESCE(pr.total_accepted, 0) as total_accepted,
                COALESCE(pr.total_rejected, 0) as total_rejected,
                (pol.po_qty - COALESCE(pr.total_received, 0)) as outstanding_qty
            FROM fct_po_line pol
            JOIN dim_item i ON pol.item_id = i.item_id
            JOIN fct_po po ON pol.po_id = po.po_id
            JOIN supplier_metrics sm ON po.supplier_id = sm.supplier_id
            LEFT JOIN po_receipts pr ON pol.po_line_id = pr.po_line_id
            WHERE pol.po_qty > COALESCE(pr.total_received, 0)
              AND sm.has_esg_report = 1
              AND i.lifecycle_status = 'Active'
        ) sub;
    """,
    "Scenario 3: Multi-Level Window Function and Deep Cost/PPV Calculation": """
        SELECT COUNT(*), MAX(CAST(cost_variance_ratio AS CHAR(255)))
        FROM (
            SELECT 
                item_category,
                item_code,
                tech_node,
                wafer_size,
                po_status,
                total_spend,
                total_ppv,
                (total_ppv / NULLIF(total_spend, 0)) as cost_variance_ratio,
                RANK() OVER (PARTITION BY item_category, tech_node ORDER BY total_ppv DESC) as ppv_rank,
                SUM(total_spend) OVER (PARTITION BY item_category ORDER BY total_spend DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_spend
            FROM (
                SELECT 
                    i.item_category,
                    i.tech_node,
                    i.wafer_size,
                    i.item_code,
                    po.po_status,
                    SUM(pol.po_qty * pol.unit_price) as total_spend,
                    SUM(pol.po_qty * (pol.unit_price - i.standard_cost)) as total_ppv
                FROM fct_po_line pol
                JOIN dim_item i ON pol.item_id = i.item_id
                JOIN fct_po po ON pol.po_id = po.po_id
                WHERE i.standard_cost > 0
                GROUP BY 
                    i.item_category,
                    i.tech_node,
                    i.wafer_size,
                    i.item_code,
                    po.po_status
            ) sub_agg
        ) sub;
    """
}

def execute_pg(config, query):
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(query)
    results = cursor.fetchall()  # This now only fetches 1 row due to the wrapper
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
    print("Starting Advanced DB Benchmark...")
    results = {db: {} for db in databases}
    
    # Warm up queries (fetch but dont measure) to put data in memory for fairness
    for name, query in queries.items():
        print(f"\nRunning {name}...")
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
        f.write("# Advanced DB Benchmark Report\n\n")
        f.write("## Scenarios Execution Time (Seconds) - Pure Analytics Load (Network Transfer Eliminated)\n\n")
        
        # Header
        f.write("| Database | " + " | ".join(queries.keys()) + " |\n")
        f.write("|----------|" + "|".join(["-"*len(q) for q in queries.keys()]) + "|\n")
        
        for db_name in databases.keys():
            row = f"| {db_name} | "
            row += " | ".join([
                f"{results[db_name][q]:.4f}s" if isinstance(results[db_name][q], float) else str(results[db_name][q]) 
                for q in queries.keys()
            ])
            row += " |\n"
            f.write(row)
            
    print("\nBenchmark complete. Report saved to benchmark_report.md")

if __name__ == "__main__":
    main()
