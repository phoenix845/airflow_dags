# Email Interactions – Airflow Data Pipeline

Story 4 — Documentation & Runbook

---

## 1. Dataset Overview

**Dataset Name:** `email_interactions`

**Purpose:**  
This dataset stores email interaction records used for analyzing:
- Email communication flow
- Email status and criticality
- Product-related discussions
- Agent effectiveness and customer satisfaction

**Source CSV Location:**
extraction/email_interactions/sample_data/email_interactions.csv

**Target PostgreSQL Table:**
public.email_interactions

---

## 2. Project Structure

extraction/email_interactions/
│
├── dags/
│ └── email_interaction_ingest.py
│
├── config/
│ ├── schema_expected.yaml
│ └── create_table.sql
│
├── sample_data/
│ └── email_interactions.csv
│
└── README.md

---

## 3. Environment Variables

The pipeline uses environment variables for database connectivity.

### `.env.sample`

```env
PG_HOST=postgres
PG_PORT=5432
PG_DB=airflow
PG_USER=airflow
PG_PASSWORD=airflow
4. How to Run the Pipeline
Step 1: Start Docker services
docker compose up -d

Step 2: Open Airflow UI
http://localhost:8080


Login credentials:

Username: airflow

Password: airflow

Step 3: Trigger the DAG

Go to DAGs

Enable email_interactions_ingest

Click Trigger DAG

5. DAG Overview

DAG Name: email_interactions_ingest

Task Flow
check_file
   ↓
transform_data
   ↓
validate_schema
   ↓
load_to_postgres

Task Description
Task Name	Description
check_file	Confirms CSV file exists
transform_data	Cleans data (trim spaces, null handling)
validate_schema	Validates columns against schema YAML
load_to_postgres	Loads data into PostgreSQL
6. Validation Rules

CSV headers must match schema_expected.yaml

Schema mismatch fails the DAG

Empty strings are converted to NULL

Duplicate records are skipped using primary key

7. Verify Data Load

Connect to PostgreSQL:

docker compose exec postgres psql -U airflow -d airflow


Run:

SELECT COUNT(*) FROM email_interactions;
SELECT * FROM email_interactions LIMIT 5;

8. Troubleshooting
DAG not visible

Ensure DAG exists under:

extraction/email_interactions/dags/


Restart Airflow:

docker compose restart airflow-webserver airflow-scheduler
Schema mismatch error

Compare CSV headers with schema_expected.yaml

Ensure column names and order are correct

CSV not found

Confirm file exists at:

extraction/email_interactions/sample_data/email_interactions.csv

Database authentication error

Verify .env credentials

Ensure they match docker-compose.yml

Clear & Re-run DAG
docker compose exec airflow-webserver airflow tasks clear \
email_interactions_ingest --yes


Then trigger the DAG again.

9. Runbook
Update Schema

Modify schema_expected.yaml

Update create_table.sql

Restart Airflow services

New CSV Drop

Replace CSV in sample_data/

Trigger DAG

Validate row count in PostgreSQL

Pre-Commit Checklist

 CSV added

 Schema YAML updated

 DDL SQL updated

 DAG runs successfully

 README updated

10. Status

✅ Story 4 Completed
✅ Pipeline reproducible end-to-end


---

## ✅ What to do now

1. Save this as  
   **`extraction/email_interactions/README.md`**
2. Commit & push
3. Finish remaining stories
4. Submit PR (as Nurdin said)

If you want, I can also:
- Write your **final PR description**
- Review **all stories checklist**
- Prepare **Story 5** if exists

Just tell me 👍
