import json
import csv
import re
import os
from pymongo import MongoClient

def normalize_amount(val):
    """
    Currency Normalization Logic:
    Convert "$10.00", "10.00", and 1000 (cents) -> 10.00 (float USD).
    """
    if val is None or val == "":
        return None

    try:
        # 1. Handle integer cents (e.g., 1000 -> 10.00)
        if isinstance(val, (int, float)):
            return float(val / 100.0)

        # 2. Handle strings (e.g., "$10.00", "10,000.00", "USD 10.00")
        if isinstance(val, str):
            # Remove symbols, letters, and commas, keeping only digits and decimal point
            clean_val = re.sub(r'[^\d.]', '', val)
            return float(clean_val) if clean_val else None

    except (ValueError, TypeError):
        return None
    return None

def clean_transactions(input_file, output_csv):
    """
    Processes raw JSONL logs into a cleaned CSV for financial reconciliation.
    """
    # Requirement: Archival to MongoDB
    # We use a 2-second timeout so the script doesn't hang if MongoDB is offline
    has_mongo = False
    try:
        mongo_client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        db = mongo_client.quickcart_audit
        archive_col = db.raw_logs
        # Ping the server to check if it's actually alive
        mongo_client.admin.command('ping')
        has_mongo = True
    except Exception:
        print("⚠️ Warning: MongoDB connection failed. Continuing with file processing only.")

    cleaned_data = []

    if not os.path.exists(input_file):
        print(f"❌ Error: Input file '{input_file}' not found.")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            # Skip empty lines
            if not line.strip():
                continue

            # Sanitization: Handle malformed JSON
            try:
                raw_record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Step 1: Archive to MongoDB (if available)
            if has_mongo:
                try:
                    archive_col.insert_one(raw_record.copy())
                except Exception:
                    has_mongo = False # Stop trying if a mid-process error occurs

            # Step 2: Navigate Nested JSON
            payload = raw_record.get('payload', {})
            entity = raw_record.get('entity', {})
            event_info = raw_record.get('event', {})
            
            event_type = event_info.get('type')
            flags = payload.get('flags') or []

            # Step 3: Filtering (Remove test/sandbox/noise)
            # Finance requires only real transactions; heartbeats are discarded.
            if 'test' in flags or event_type == 'heartbeat' or 'noise' in flags:
                continue

            # Step 4: Normalization
            amount_raw = payload.get('Amount')
            amount_usd = normalize_amount(amount_raw)
            payment_id = entity.get('payment', {}).get('id')

            # Step 5: Sanitization (Drop incomplete/unrecoverable records)
            if amount_usd is not None and payment_id:
                cleaned_data.append({
                    'payment_id': payment_id,
                    'order_id': entity.get('order', {}).get('id'),
                    'amount_usd': amount_usd,
                    'status': payload.get('status'),
                    'ts': event_info.get('ts')
                })

    # Step 6: Output finance-grade CSV
    if cleaned_data:
        keys = ['payment_id', 'order_id', 'amount_usd', 'status', 'ts']
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(cleaned_data)
        print(f"✅ Success: {len(cleaned_data)} cleaned transactions saved to {output_csv}")
    else:
        print("⚠️ No valid transactions were found to clean.")

if __name__ == "__main__":
    # Adjust path to your local environment as needed
    input_path = 'raw_data.jsonl'
    output_path = 'clean_transactions.csv'
    clean_transactions(input_path, output_path)