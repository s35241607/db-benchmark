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
    "Scenario 1: Extreme 10-Wide-Table Deep Join & Aggregation": """
        SELECT *
        FROM rpt_extreme_join;
    """,
    "Scenario 2: Extreme Deep CTEs & Pre-Aggregation Cross Analysis": """
        SELECT *
        FROM rpt_extreme_cte;
    """,
    "Scenario 3: Extreme Multi-Level Window Functions on 300K Union Rows": """
        SELECT *
        FROM rpt_extreme_window;
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
    return end - start, len(results)

def execute_mysql(config, query):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end = time.time()
    cursor.close()
    conn.close()
    return end - start, len(results)

def execute_clickhouse(config, query):
    client = Client(**config)
    start = time.time()
    results = client.execute(query)
    end = time.time()
    client.disconnect()
    return end - start, len(results)

databases = {
    "PostgreSQL (Default)": (execute_pg, CONFIG_PG_DEFAULT),
    "PostgreSQL (OLAP Tuned)": (execute_pg, CONFIG_PG_OLAP),
    "ClickHouse": (execute_clickhouse, CONFIG_CLICKHOUSE),
    "StarRocks": (execute_mysql, CONFIG_STARROCKS),
}

def main():
    print("Starting Advanced DB Benchmark...")
    results = {db: {} for db in databases}
    counts = {db: {} for db in databases}
    
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
                row_count = 0
                for _ in range(runs):
                    duration, count = func(config, query)
                    time_taken += duration
                    row_count = count # Same query should return same count
                
                avg_time = time_taken / runs
                results[db_name][name] = avg_time
                counts[db_name][name] = row_count
                print(f"  [{db_name}] Avg Time: {avg_time:.4f}s | Rows: {row_count}")
            except Exception as e:
                print(f"  [{db_name}] ERROR: {e}")
                results[db_name][name] = "ERROR"
                counts[db_name][name] = "-"

    # Write report
    with open("benchmark_report.md", "w") as f:
        f.write("# Advanced DB Benchmark Report\n\n")
        f.write("## Scenarios Execution Time and Row Counts\n\n")
        
        # Header
        f.write("| Database | " + " | ".join(queries.keys()) + " |\n")
        f.write("|----------|" + "|".join(["-"*len(q) for q in queries.keys()]) + "|\n")
        
        for db_name in databases.keys():
            row = f"| {db_name} | "
            cols = []
            for q in queries.keys():
                time_val = results[db_name][q]
                count_val = counts[db_name][q]
                if isinstance(time_val, float):
                    cols.append(f"**{count_val}** rows<br>({time_val:.4f}s)")
                else:
                    cols.append(str(time_val))
            row += " | ".join(cols)
            row += " |\n"
            f.write(row)
            
    print("\nBenchmark complete. Report saved to benchmark_report.md")

if __name__ == "__main__":
    main()
