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

- **Raw Layer (`raw/`)**: Stores source files in their native, unaltered form to ensure full traceability and auditability.
- **Entity & Date Partitioning**: Files are partitioned by entity (`customers/`, `orders/`) using standard date stamps (`<entity>_YYYY_MM_DD.csv`) for predictable daily batch ingestion.
- **Processed Layer (`processed/`)**: Target directory for validated or transformed batch files.

---

## 2. Technical Concepts: Bucket vs. Object vs. Prefix

| Component | Architecture Role | Characteristics |
| :--- | :--- | :--- |
| **Bucket** | Top-level Container | The foundational storage namespace in object storage (similar to a drive or volume). Buckets govern access controls, regions, lifecycle policies, and encryption settings. |
| **Object** | Atomic Data Unit | The fundamental storage entity containing data bytes, associated metadata (size, content type, ETag), and a unique key name. Objects are immutable—they cannot be updated in-place and must be rewritten. |
| **Prefix** | Virtual Directory Hierarchy | Object stores use a completely flat namespace without physical directories. A prefix is a string key delimiter (using `/`) that client tools and user interfaces parse to display virtual directory trees. |

---

## 3. Post-Processing Strategy & File Lifecycle

1. **Strict Immutability**: Source files in `raw/` are never overwritten or deleted immediately upon pipeline completion to maintain a complete historical audit log.
2. **Transfer to Processed**: Once an Airflow batch run completes all staging transformations and passes data quality checks, files are archived into `processed/<entity>/YYYY/MM/` or tagged with ingestion metadata.
3. **Automated Lifecycle Management (ILM)**:
   - **Hot Storage**: Files remain in `raw/` for 30–60 days for reprocessing or pipeline recovery.
   - **Cold Archival**: Automated MinIO lifecycle rules transition files older than 60 days to compressed, lower-cost archival tiers.
   - **Retention**: Compliance-regulated expiration policies purge files only after statutory retention limits expire.