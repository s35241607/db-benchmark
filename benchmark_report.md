# Advanced DB Benchmark Report

## Scenarios Execution Time (Seconds) - Pure Analytics Load (Network Transfer Eliminated)

| Database | Scenario 1: Complex 11-Table Join for Semiconductor Supply Chain Yield Risk | Scenario 2: Advanced CTE Array-Like Supply vs Payment Terms Analysis | Scenario 3: Multi-Level Window Function and Deep Cost/PPV Calculation |
|----------|---------------------------------------------------------------------------|--------------------------------------------------------------------|---------------------------------------------------------------------|
| PostgreSQL (Default) | 0.0695s | 0.0906s | 0.3297s |
| PostgreSQL (OLAP Tuned) | 0.0689s | 0.0885s | 0.1152s |
| ClickHouse | 0.1755s | 0.0931s | 0.0931s |
| StarRocks | 0.0729s | 0.0470s | 0.0362s |
