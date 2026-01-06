# 📄 MANIFEST.md  
## Dataset: Email Interactions

---

### Dataset Name
**email_interactions**

---

### Dataset Description
The **email_interactions** dataset captures structured records of email communications exchanged between senders and receivers.  
It is used for analyzing:

- Email communication flow  
- Email state, type, and criticality  
- Product-related discussions  
- Agent efficiency, effectiveness, and customer satisfaction  

This dataset supports downstream analytics and reporting use cases related to operational efficiency and customer engagement.

---

### Source
- **Origin:** SharePoint (approved dataset with analysis status = *Complete*)
- **Ingestion Method:** Local CSV download (manual)
- **Format:** CSV

---

### Local CSV Location
extraction/email_interactions/sample_data/email_interactions.csv

---

### Target Database
- **Database Type:** PostgreSQL
- **Schema:** `public`
- **Target Table:** `email_interactions`

---

### Primary Key
thread_id

---

### Data Contract
- **Schema Definition:** `config/schema_expected.yaml`
- **DDL Definition:** `config/create_table.sql`
- CSV files **must conform exactly** to the schema contract.
- Schema validation failures will cause the Airflow DAG to fail.

---

### Processing Notes
- Empty strings are normalized to `NULL`
- Whitespace is stripped from string fields
- Duplicate records are prevented using the primary key (`thread_id`)
- Rows already marked with status `done` (if present) are skipped during ingestion

---

### Ownership
- **Pipeline Owner:** Data Engineering Team
- **Maintainer:** Internship Project — Local Airflow Pipelines

---

### Change Management
Any change to the dataset (schema updates, new columns, or CSV structure) requires:
1. Updating `schema_expected.yaml`
2. Updating `create_table.sql`
3. Validating DAG execution
4. Updating documentation as needed

---

### Status
✅ Dataset ready for ingestion  
✅ Schema contract defined  
✅ DDL validated  
✅ Pipeline compatible  

---

### Reviewer Checklist
- [x] Dataset name clearly defined  
- [x] CSV path documented  
- [x] Target table specified  
- [x] Schema & DDL linked  
- [x] Processing rules documented  
