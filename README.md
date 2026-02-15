# QuickCart Data Integrity Audit: Establishing a Financial Source of Truth

**Student Name:** Nuria Hatibu</br>
**Student ID:** ALT/SOD/025/3085</br>
**Program:** Data Engineering (Karatu 2025 Semester)</br>
**Project:** Capstone — The Data Integrity Crisis</br>

## 1. Project Overview</br>
QuickCart, an e-commerce startup, experienced a P0 incident where the Marketing "Total Sales" dashboard deviated significantly from Bank Settlement statements. This project establishes an auditable, finance-grade **Source of Truth** by reconciling three conflicting data sources:</br>
* **Raw JSON Transaction Logs:** Messy, nested, and inconsistently formatted.</br>
* **Production Database:** Containing orders and payment retry attempts.</br>
* **Bank Settlements:** The definitive record of money received.</br>

## 2. Problem Statement</br>
The audit addressed two primary "broken" sources of truth:</br>
1. **JSON Log Inconsistency:** Currencies were stored as strings with symbols (e.g., "$10.00"), plain strings ("10.00"), or integer cents (1000). Sandbox/test transactions were also co-mingled with production data.</br>
2. **Database Integrity:** Orders lacked a 1-to-1 relationship with payments due to retries, and "orphan" payments existed in the bank statement with no corresponding internal order records.

## 3. Solution Architecture</br>
### Part A: Python Data Cleaning & Standardization (`clean_transactions.py`)</br>
The Python script serves as the ETL layer. It navigates nested dictionaries, standardizes currency via Regular Expressions, and filters out non-commercial noise.</br>
* **Currency Normalization:** Converts all formats into a standardized `amount_usd` float.</br>
* **Filtering:** Removes records flagged as "test" or "sandbox" and "heartbeat" events.</br>
* **Sanitization:** Drops records missing critical identifiers like `payment_id`.</br>
* **Archival:** Implements a protocol to write raw JSON logs to **MongoDB** for long-term audit compliance.</br>

### Part B: SQL Data Reconciliation (`reconciliation.sql`)</br>
The SQL layer uses advanced analytical techniques to resolve structural discrepancies within the PostgreSQL database.</br>

* **CTEs (Common Table Expressions):** Used to create "Internal Truth" and "Bank Truth" staging layers.</br>
* **Window Functions:** Utilizes `ROW_NUMBER()` to deduplicate multiple payment attempts per order, ensuring revenue is only counted once upon the latest successful attempt.</br>
* **Join Logic:** Employs `LEFT JOINs` to identify **Orphan Payments**—funds settled in the bank that are missing from the internal order management system.</br>

## 4. Final Reconciliation Results</br>
The following figures represent the audited financial state of QuickCart for the period:</br>

| Metric | Value (USD) |</br>
| --- | --- |</br>
| **Total Successful Sales** | **$5,269,910.24** |</br>
| **Orphan Payments** | **$525,705.09** |</br>
| **Discrepancy Gap** | **-$1,168,553.02** |</br>

## 5. Technical Implementation & Usage</br>

### Prerequisites</br>

* Python 3.12+</br>
* PostgreSQL 15</br>
* PyMongo (for archival logic)</br>

### Installation</br>
1. Clone the repository:</br>
```bash</br>
git clone https://github.com/nuriahatibu/secondsemester.git</br>
```
2. Install dependencies:</br>
```bash
pip install pymongo pandas
```

### Execution
1. **Run the Python ETL:**
```bash
python clean_transactions.py
```

2. **Initialize Database & Load Seeds:**
```powershell
psql -d quickcart_audit -f schema.sql
psql -d quickcart_audit -f seed_orders.sql
psql -d quickcart_audit -f seed_payments.sql
psql -d quickcart_audit -f seed_bank_settlements.sql
```

3. **Run the Reconciliation Report:**
```powershell
psql -d secondsemester -f reconciliation.sql
```

## 6. Conclusion
The audit reveals that while the internal system recorded **$5.27M** in successful sales, the bank settled significantly more, leading to a negative discrepancy gap. This suggests a failure in the internal system's ability to record "Success" callbacks for all processed transactions. These results provide a defensible foundation for the Finance team to close the month and resolve the P0 incident.
