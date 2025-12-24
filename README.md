# 🚚 Data Engineering: Delivery Service Analytics Pipeline

delivery-project/
├── airflow/
│   ├── dags/
│   │   ├── delivery_normalization.py
│   │   └── delivery_datamarts.py
│   ├── data/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   └── requirements.txt
├── postgres/
│   └── init_delivery_db.sql
├── pgadmin/
│   └── servers.json
├── docker-compose.yaml
├── .gitignore
└── README.md

## System Architecture

### Data Flow

### 🗄️ Database Schema (3NF)
**Core Tables:**
- `users` - Customers (`user_id`, `user_phone`)
- `stores` - Store locations (`store_id`, `store_address`)
- `drivers` - Delivery personnel (`driver_id`, `driver_phone`)
- `items` - Product catalog (`item_id`, `item_title`, `item_category`)
- `orders` - Order transactions
- `order_items` - Line items in orders

**Analytical Datamarts:**
- `dm_orders` - Daily order metrics by city/store
- `dm_items` - Product performance metrics

---

## ⚙️ Technical Implementation

### 🔄 ETL Pipeline
**DAG 1: `delivery_normalization`**
- **Input:** `deliveries.parquet` (9.5M rows)
- **Process:** Splits data into 6 normalized tables
- **Output:** PostgreSQL database

**DAG 2: `delivery_datamarts`**
- **Input:** Normalized tables
- **Process:** Aggregates metrics
- **Output:** Two datamarts (`dm_orders`, `dm_items`)

---

## 🐳 Infrastructure & Deployment

### 🏗️ Tech Stack
- **Orchestration:** Apache Airflow 2.10.2
- **Database:** PostgreSQL 15
- **Containerization:** Docker Compose
- **Data Processing:** Pandas + PyArrow
- **Admin Interface:** PgAdmin 4

### 🚀 Quick Start
```bash
# 1. Clone repository
git clone https://github.com/Valeriieee12/DE.git
cd DE
git checkout final_project

# 2. Add your data file
# Place deliveries.parquet in airflow/data/

# 3. Start services
docker-compose up --build

# 4. Access:
# Airflow: http://localhost:8080 (admin/admin)
# PgAdmin: http://localhost:5050 (delivery@admin.com/pgadmin_pass_123)
```

delivery-project/
├── airflow/
│   ├── dags/
│   │   ├── delivery_normalization.py
│   │   └── delivery_datamarts.py
│   ├── data/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   └── requirements.txt
├── postgres/
│   └── init_delivery_db.sql
├── pgadmin/
│   └── servers.json
├── docker-compose.yaml
├── .gitignore
└── README.md
