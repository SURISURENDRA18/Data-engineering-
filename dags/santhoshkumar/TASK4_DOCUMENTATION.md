# Task 4 — Data Lake Architecture with MinIO

## 1. MinIO Object Hierarchy & Data Lake Design
The Data Lake storage layer is configured in MinIO within bucket `test` under namespace `santhoshkumar_datalake/`:

```text
test/santhoshkumar_datalake/
├── raw/
│   ├── customers/
│   │   ├── customers_2026_09_15.csv
│   │   └── customers_2026_09_16.csv
│   └── orders/
│       ├── orders_2026_09_15.csv
│       └── orders_2026_09_16.csv
└── processed/```