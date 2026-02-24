# Advanced DB Benchmark Report

## Scenarios Execution Time and Row Counts

| Database | Scenario 1: Extreme 10-Wide-Table Deep Join & Aggregation | Scenario 2: Extreme Deep CTEs & Pre-Aggregation Cross Analysis | Scenario 3: Extreme Multi-Level Window Functions on 300K Union Rows |
|----------|---------------------------------------------------------|--------------------------------------------------------------|-------------------------------------------------------------------|
| PostgreSQL (Default) | **100** rows<br>(1.3415s) | **100** rows<br>(0.2949s) | **300000** rows<br>(21.0004s) |
| PostgreSQL (OLAP Tuned) | **100** rows<br>(1.6274s) | **100** rows<br>(0.1957s) | **300000** rows<br>(19.5433s) |
| ClickHouse | **100** rows<br>(1.4886s) | **100** rows<br>(0.2317s) | **300000** rows<br>(9.5796s) |
| StarRocks | **100** rows<br>(0.5164s) | **100** rows<br>(0.0856s) | **300000** rows<br>(26.0216s) |
