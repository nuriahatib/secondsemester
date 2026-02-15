#!/usr/bin/env python3
"""
QuickCart synthetic data generator (no external deps).

Outputs:
- raw_data.jsonl               (nested transaction logs)
- seed_orders.sql              (orders table inserts)
- seed_payments.sql            (payments table inserts)
- seed_bank_settlements.sql    (bank settlements table inserts)

Designed to include:
- Mixed amount formats: "$10.00" strings, integer cents, missing values
- Test transactions
- Multiple payment attempts (retries) per order
- Orphan payments (no associated order)
- Duplicate bank settlement lines + settlements missing internal refs
"""

import argparse
import json
import os
import random
import string
from datetime import datetime, timedelta, UTC
from uuid import uuid4

# ----------------------------- helpers -----------------------------

def rand_choice_weighted(pairs):
    """pairs = [(value, weight), ...]"""
    total = sum(w for _, w in pairs)
    r = random.uniform(0, total)
    upto = 0
    for v, w in pairs:
        upto += w
        if upto >= r:
            return v
    return pairs[-1][0]

def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def money_cents_from_total(total_cents: int) -> int:
    # allow weirdness: sometimes negative or zero sneaks in as "bad data"
    if random.random() < 0.002:
        return random.choice([0, -total_cents, -500])
    return total_cents

def format_amount_messy(total_cents: int):
    """
    Return amount in one of:
      - "$10.00" (string)
      - 1000 (int cents)
      - "10.00" (string without symbol)
      - None (missing)
      - "" (empty)
    """
    mode = rand_choice_weighted([
        ("usd_symbol", 0.45),
        ("int_cents", 0.35),
        ("plain_string", 0.10),
        ("missing", 0.07),
        ("empty", 0.03),
    ])

    if mode == "missing":
        return None
    if mode == "empty":
        return ""

    if mode == "int_cents":
        return money_cents_from_total(total_cents)

    # string formats
    dollars = total_cents / 100.0
    if mode == "plain_string":
        # sometimes commas appear
        s = f"{dollars:,.2f}" if random.random() < 0.2 else f"{dollars:.2f}"
        return s

    # "$" format
    s = f"{dollars:,.2f}" if random.random() < 0.35 else f"{dollars:.2f}"
    # sometimes space or weird prefix
    prefix = rand_choice_weighted([("$", 0.85), ("USD ", 0.10), ("$ ", 0.05)])
    return f"{prefix}{s}"

def random_email():
    user = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(random.randint(6, 12)))
    dom = random.choice(["gmail.com", "yahoo.com", "outlook.com", "quickcart.test", "example.com"])
    return f"{user}@{dom}"

def provider_ref():
    return "prov_" + uuid4().hex[:18]

def sql_escape(s: str) -> str:
    return s.replace("'", "''")

# ----------------------------- generation -----------------------------

def generate(args):
    random.seed(args.seed)

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    start_dt = datetime.now(UTC) - timedelta(days=args.days)
    end_dt = datetime.now(UTC)

    # Core IDs
    order_ids = []
    order_rows = []
    payment_rows = []
    bank_rows = []
    log_events = []

    # For linking + anomalies
    payment_ids_by_order = {}
    all_payment_ids = []

    # Orders
    for i in range(args.orders):
        oid = f"ord_{uuid4().hex[:16]}"
        order_ids.append(oid)

        created_at = start_dt + timedelta(seconds=random.randint(0, int((end_dt - start_dt).total_seconds())))
        customer_id = f"cus_{uuid4().hex[:12]}"
        email = random_email()

        # cart total in cents
        subtotal = random.randint(500, 25000)     # $5 to $250
        shipping = random.choice([0, 300, 500, 800, 1200])
        tax = int(subtotal * random.choice([0.0, 0.03, 0.05, 0.075, 0.1]))
        total_cents = subtotal + shipping + tax

        is_test = 1 if (random.random() < args.test_rate) else 0
        # some "test" is not obvious: use emails/domain flag + explicit flag
        if random.random() < 0.35 and is_test == 1:
            email = f"test_{email}"

        order_rows.append({
            "order_id": oid,
            "customer_id": customer_id,
            "customer_email": email,
            "order_total_cents": total_cents,
            "currency": "USD",
            "is_test": is_test,
            "created_at": iso(created_at),
        })

    # Payments (attempts)
    for oid in order_ids:
        # attempts: most have 1, some have 2-4 retries
        attempts = rand_choice_weighted([(1, 0.72), (2, 0.20), (3, 0.06), (4, 0.02)])
        payment_ids_by_order[oid] = []

        order_total = next(r["order_total_cents"] for r in order_rows if r["order_id"] == oid)
        created_at = datetime.strptime(next(r["created_at"] for r in order_rows if r["order_id"] == oid), "%Y-%m-%dT%H:%M:%SZ")

        for a in range(attempts):
            pid = f"pay_{uuid4().hex[:16]}"
            payment_ids_by_order[oid].append(pid)
            all_payment_ids.append(pid)

            attempted_at = created_at + timedelta(minutes=random.randint(1, 240), seconds=random.randint(0, 59))

            # status distribution
            status = rand_choice_weighted([
                ("FAILED", 0.18),
                ("PENDING", 0.07),
                ("SUCCESS", 0.75),
            ])

            # if earlier attempt succeeded, later attempts should often be failed/duplicate noise
            if a > 0 and any(p.get("status") == "SUCCESS" for p in payment_rows if p.get("order_id") == oid):
                status = rand_choice_weighted([("FAILED", 0.70), ("SUCCESS", 0.20), ("PENDING", 0.10)])

            amount_cents = order_total

            payment_rows.append({
                "payment_id": pid,
                "order_id": oid,
                "attempt_no": a + 1,
                "provider": random.choice(["stripe", "paypal", "flutterwave"]),
                "provider_ref": provider_ref(),
                "status": status,
                "amount_cents": amount_cents,
                "attempted_at": iso(attempted_at),
            })

    # Orphan payments (payments that exist without orders)
    orphan_count = int(args.orphan_payment_rate * len(payment_rows))
    for _ in range(orphan_count):
        pid = f"pay_{uuid4().hex[:16]}"
        all_payment_ids.append(pid)

        attempted_at = start_dt + timedelta(seconds=random.randint(0, int((end_dt - start_dt).total_seconds())))
        amount_cents = random.randint(500, 30000)
        status = rand_choice_weighted([("SUCCESS", 0.65), ("FAILED", 0.25), ("PENDING", 0.10)])

        payment_rows.append({
            "payment_id": pid,
            "order_id": None,
            "attempt_no": 1,
            "provider": random.choice(["stripe", "paypal", "flutterwave"]),
            "provider_ref": provider_ref(),
            "status": status,
            "amount_cents": amount_cents,
            "attempted_at": iso(attempted_at),
        })

    # Bank settlements (authoritative money-in)
    # mostly from SUCCESS payments, but includes duplicates + missing refs
    success_payments = [p for p in payment_rows if p["status"] == "SUCCESS"]
    sample_size = min(len(success_payments), args.bank_rows)
    bank_sample = random.sample(success_payments, sample_size)

    for p in bank_sample:
        settled_at = datetime.strptime(p["attempted_at"], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=random.randint(1, 72))
        settle_id = f"set_{uuid4().hex[:16]}"

        # sometimes bank settles partial or slightly off (fees, rounding, weirdness)
        amt = p["amount_cents"]
        if random.random() < args.partial_settlement_rate:
            amt = int(amt * random.choice([0.5, 0.8, 0.9]))

        bank_rows.append({
            "settlement_id": settle_id,
            "payment_id": p["payment_id"] if random.random() > args.bank_missing_payment_id_rate else None,
            "provider_ref": p["provider_ref"] if random.random() > args.bank_missing_provider_ref_rate else None,
            "status": "SETTLED",
            "settled_amount_cents": amt,
            "currency": "USD",
            "settled_at": iso(settled_at),
        })

        # duplicates in bank statement
        if random.random() < args.bank_duplicate_rate:
            dup = dict(bank_rows[-1])
            dup["settlement_id"] = f"set_{uuid4().hex[:16]}"
            bank_rows.append(dup)

    # Raw JSON logs (nested + messy)
    # Create log events from payments, plus noise
    for p in payment_rows:
        event_time = datetime.strptime(p["attempted_at"], "%Y-%m-%dT%H:%M:%SZ")
        amount_field = format_amount_messy(p["amount_cents"])

        # some logs lack order_id even for valid payments
        order_id = p["order_id"]
        if random.random() < args.log_missing_order_id_rate:
            order_id = None

        # test flags appear in different ways
        flags = []
        if random.random() < 0.10:
            flags.append("replayed")
        if random.random() < args.test_rate:
            flags.append("test")

        log_events.append({
            "event": {
                "id": f"evt_{uuid4().hex[:18]}",
                "type": rand_choice_weighted([("payment_attempted", 0.45), ("payment_succeeded", 0.40), ("payment_failed", 0.15)]),
                "ts": iso(event_time),
                "source": rand_choice_weighted([("web", 0.55), ("mobile", 0.35), ("internal", 0.10)]),
            },
            "entity": {
                "order": {"id": order_id},
                "payment": {"id": p["payment_id"], "provider_ref": p["provider_ref"], "provider": p["provider"]},
                "customer": {"email": random_email()},
            },
            "payload": {
                "Amount": amount_field,  # intentionally inconsistent
                "currency": "USD",
                "status": p["status"],
                "flags": flags if flags else None,
                "metadata": {
                    "ip": ".".join(str(random.randint(1, 254)) for _ in range(4)),
                    "user_agent": random.choice(["Chrome", "Safari", "Firefox", "Edge", "MobileApp"]),
                }
            }
        })

    # Extra pure-noise events
    noise_events = int(args.log_noise_rate * len(log_events))
    for _ in range(noise_events):
        t = start_dt + timedelta(seconds=random.randint(0, int((end_dt - start_dt).total_seconds())))
        log_events.append({
            "event": {"id": f"evt_{uuid4().hex[:18]}", "type": "heartbeat", "ts": iso(t), "source": "internal"},
            "entity": {"order": {"id": None}, "payment": {"id": None}, "customer": {"email": None}},
            "payload": {"Amount": None, "currency": "USD", "status": None, "flags": ["noise"]}
        })

    random.shuffle(log_events)

    # ----------------------------- write files -----------------------------

    # JSONL logs
    raw_path = os.path.join(outdir, "raw_data.jsonl")
    with open(raw_path, "w", encoding="utf-8") as f:
        for ev in log_events:
            f.write(json.dumps(ev) + "\n")

    # SQL seed files
    orders_sql = os.path.join(outdir, "seed_orders.sql")
    payments_sql = os.path.join(outdir, "seed_payments.sql")
    bank_sql = os.path.join(outdir, "seed_bank_settlements.sql")

    with open(orders_sql, "w", encoding="utf-8") as f:
        f.write("-- seed_orders.sql\n")
        for r in order_rows:
            f.write(
                "INSERT INTO orders (order_id, customer_id, customer_email, order_total_cents, currency, is_test, created_at) VALUES "
                f"('{r['order_id']}', '{r['customer_id']}', '{sql_escape(r['customer_email'])}', {r['order_total_cents']}, '{r['currency']}', {r['is_test']}, '{r['created_at']}');\n"
            )

    with open(payments_sql, "w", encoding="utf-8") as f:
        f.write("-- seed_payments.sql\n")
        for r in payment_rows:
            order_id_sql = "NULL" if r["order_id"] is None else f"'{r['order_id']}'"
            f.write(
                "INSERT INTO payments (payment_id, order_id, attempt_no, provider, provider_ref, status, amount_cents, attempted_at) VALUES "
                f"('{r['payment_id']}', {order_id_sql}, {r['attempt_no']}, '{r['provider']}', '{r['provider_ref']}', '{r['status']}', {r['amount_cents']}, '{r['attempted_at']}');\n"
            )

    with open(bank_sql, "w", encoding="utf-8") as f:
        f.write("-- seed_bank_settlements.sql\n")
        for r in bank_rows:
            pid_sql = "NULL" if r["payment_id"] is None else f"'{r['payment_id']}'"
            pref_sql = "NULL" if r["provider_ref"] is None else f"'{r['provider_ref']}'"
            f.write(
                "INSERT INTO bank_settlements (settlement_id, payment_id, provider_ref, status, settled_amount_cents, currency, settled_at) VALUES "
                f"('{r['settlement_id']}', {pid_sql}, {pref_sql}, '{r['status']}', {r['settled_amount_cents']}, '{r['currency']}', '{r['settled_at']}');\n"
            )

    # Summary
    print("✅ Generated:")
    print(f"  - {raw_path}  (events: {len(log_events):,})")
    print(f"  - {orders_sql} (orders: {len(order_rows):,})")
    print(f"  - {payments_sql} (payments: {len(payment_rows):,})")
    print(f"  - {bank_sql} (bank rows: {len(bank_rows):,})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="quickcart_data")
    ap.add_argument("--seed", type=int, default=7)

    ap.add_argument("--orders", type=int, default=50_000)
    ap.add_argument("--bank-rows", type=int, default=70_000)
    ap.add_argument("--days", type=int, default=30)

    ap.add_argument("--test-rate", type=float, default=0.06)  # % of orders flagged test-ish
    ap.add_argument("--orphan-payment-rate", type=float, default=0.05)  # relative to payment rows
    ap.add_argument("--partial-settlement-rate", type=float, default=0.03)

    ap.add_argument("--bank-duplicate-rate", type=float, default=0.02)
    ap.add_argument("--bank-missing-payment-id-rate", type=float, default=0.03)
    ap.add_argument("--bank-missing-provider-ref-rate", type=float, default=0.02)

    ap.add_argument("--log-missing-order-id-rate", type=float, default=0.04)
    ap.add_argument("--log-noise-rate", type=float, default=0.03)

    args = ap.parse_args([])
    generate(args)

if __name__ == "__main__":
    main()