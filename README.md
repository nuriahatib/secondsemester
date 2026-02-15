# QuickCart Data Integrity Audit: Establishing a Financial Source of Truth

**Student Name:** Nuria Hatibu
**Student ID:** ALT/SOD/025/3085
**Program:** Data Engineering (Karatu 2025 Semester)
**Project:** Capstone — The Data Integrity Crisis

## 1. Project Overview
QuickCart, an e-commerce startup, experienced a P0 incident where the Marketing "Total Sales" dashboard deviated significantly from Bank Settlement statements. This project establishes an auditable, finance-grade **Source of Truth** by reconciling three conflicting data sources:
* **Raw JSON Transaction Logs:** Messy, nested, and inconsistently formatted.
* **Production Database:** Containing orders and payment retry attempts.
* **Bank Settlements:** The definitive record of money received.

## 2. Problem Statement

The audit addressed two primary "broken" sources of truth:

1. **JSON Log Inconsistency:** Currencies were stored as strings with symbols (e.g., "$10.00"), plain strings ("10.00"), or integer cents (1000). Sandbox/test transactions were also co-mingled with production data.
2. **Database Integrity:** Orders lacked a 1-to-1 relationship with payments due to retries, and "orphan" payments existed in the bank statement with no corresponding internal order records.

## 3. Solution Architecture

### Part A: Python Data Cleaning & Standardization (`clean_transactions.py`)
The Python script serves as the ETL layer. It navigates nested dictionaries, standardizes currency via Regular Expressions, and filters out non-commercial noise.
* **Currency Normalization:** Converts all formats into a standardized `amount_usd` float.
* **Filtering:** Removes records flagged as "test" or "sandbox" and "heartbeat" events.
* **Sanitization:** Drops records missing critical identifiers like `payment_id`.
* **Archival:** Implements a protocol to write raw JSON logs to **MongoDB** for long-term audit compliance.

### Part B: SQL Data Reconciliation (`reconciliation.sql`)

The SQL layer uses advanced analytical techniques to resolve structural discrepancies within the PostgreSQL database.

* **CTEs (Common Table Expressions):** Used to create "Internal Truth" and "Bank Truth" staging layers.
* **Window Functions:** Utilizes `ROW_NUMBER()` to deduplicate multiple payment attempts per order, ensuring revenue is only counted once upon the latest successful attempt.
* **Join Logic:** Employs `LEFT JOINs` to identify **Orphan Payments**—funds settled in the bank that are missing from the internal order management system.

## 4. Final Reconciliation Results

The following figures represent the audited financial state of QuickCart for the period:

| Metric | Value (USD) |
| --- | --- |
| **Total Successful Sales** | **$5,269,910.24** |
| **Orphan Payments** | **$525,705.09** |
| **Discrepancy Gap** | **-$1,168,553.02** |

## 5. Technical Implementation & Usage

### Prerequisites

* Python 3.12+
* PostgreSQL 15
* PyMongo (for archival logic)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/nuriahatibu/secondsemester.git
```

2. Install dependencies:
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
