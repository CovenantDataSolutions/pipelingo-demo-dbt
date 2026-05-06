"""Generate the 4 supplemental seed CSVs with realistic distributions.

These tables don't exist in TPC-H, so we synthesize them. Keys reference
real TPC-H custkeys and orderkeys so joins work properly.

Run from the project root:
    python scripts/generate_seeds.py

This is idempotent — same seed always produces same CSVs.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)  # reproducible output

OUT = Path(__file__).parent.parent / "seeds"
OUT.mkdir(exist_ok=True)

# TPC-H key ranges at SF1 scale
N_CUSTOMERS = 150_000
N_ORDERS = 1_500_000

# We'll generate seeds that reference a manageable subset (1000 customers,
# 5000 orders) so the joins are dense enough to feel real but the seeds
# load fast.
SAMPLE_CUSTKEYS = sorted(random.sample(range(1, N_CUSTOMERS + 1), 1000))
SAMPLE_ORDERKEYS = sorted(random.sample(range(1, N_ORDERS + 1), 5000))

# ----- raw_payments -----
PAYMENT_METHODS = ["credit_card", "debit_card", "ach", "paypal", "apple_pay"]
PAYMENT_STATUSES = ["captured", "captured", "captured", "captured", "captured", "pending", "failed", "refunded"]

with (OUT / "raw_payments.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["payment_id", "order_id", "customer_id", "amount_usd", "payment_method", "status", "paid_at"])
    for orderkey in SAMPLE_ORDERKEYS:
        if random.random() < 0.85:  # 85% of sampled orders have a payment
            payment_id = f"PMT_{orderkey:08d}_{random.randint(1000, 9999)}"
            custkey = random.choice(SAMPLE_CUSTKEYS)
            amount = round(random.uniform(15, 5000), 2)
            method = random.choice(PAYMENT_METHODS)
            status = random.choice(PAYMENT_STATUSES)
            days_ago = random.randint(1, 365)
            paid_at = (datetime(2025, 12, 31) - timedelta(days=days_ago, hours=random.randint(0, 23))).isoformat()
            w.writerow([payment_id, orderkey, custkey, amount, method, status, paid_at])

# ----- raw_shipments -----
CARRIERS = ["UPS", "FedEx", "USPS", "DHL"]
SHIP_STATUSES = ["delivered", "delivered", "delivered", "in_transit", "delayed", "returned"]

with (OUT / "raw_shipments.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["shipment_id", "order_id", "carrier", "tracking_number", "status", "shipped_at", "delivered_at"])
    for orderkey in SAMPLE_ORDERKEYS:
        if random.random() < 0.75:  # 75% of sampled orders are shipped
            shipment_id = f"SHIP_{orderkey:08d}"
            carrier = random.choice(CARRIERS)
            tracking = f"{carrier[:2].upper()}{random.randint(100000000, 999999999)}"
            status = random.choice(SHIP_STATUSES)
            days_ago = random.randint(1, 360)
            shipped_at = datetime(2025, 12, 31) - timedelta(days=days_ago)
            delivered_at = shipped_at + timedelta(days=random.randint(2, 8)) if status == "delivered" else ""
            w.writerow([
                shipment_id, orderkey, carrier, tracking, status,
                shipped_at.isoformat(),
                delivered_at.isoformat() if delivered_at else "",
            ])

# ----- raw_marketing_campaigns -----
CHANNELS = ["google_ads", "facebook", "email", "instagram", "tiktok", "podcast"]
CAMPAIGN_TYPES = ["acquisition", "retention", "reactivation", "brand"]

with (OUT / "raw_marketing_campaigns.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["campaign_id", "campaign_name", "channel", "campaign_type", "budget_usd", "started_at", "ended_at"])
    for i in range(1, 41):  # 40 campaigns over 2 years
        ch = random.choice(CHANNELS)
        ct = random.choice(CAMPAIGN_TYPES)
        budget = random.choice([2500, 5000, 10000, 25000, 50000, 100000])
        days_ago = random.randint(30, 730)
        start = datetime(2025, 12, 31) - timedelta(days=days_ago)
        end = start + timedelta(days=random.randint(7, 90))
        w.writerow([
            f"CMP_{i:04d}",
            f"{ct.title()} {ch.replace('_', ' ').title()} Q{((start.month - 1) // 3) + 1}",
            ch, ct, budget,
            start.isoformat(), end.isoformat(),
        ])

# ----- raw_marketing_clicks -----
with (OUT / "raw_marketing_clicks.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["click_id", "campaign_id", "customer_id", "clicked_at", "source_url", "converted"])
    for i in range(1, 8001):  # 8K clicks
        campaign = f"CMP_{random.randint(1, 40):04d}"
        # ~70% of clicks attributed to a known customer; rest are anonymous (NULL)
        custkey = random.choice(SAMPLE_CUSTKEYS) if random.random() < 0.7 else ""
        days_ago = random.randint(1, 700)
        clicked = datetime(2025, 12, 31) - timedelta(days=days_ago, hours=random.randint(0, 23))
        # ~12% of clicks convert to a purchase (industry-realistic)
        converted = "true" if random.random() < 0.12 else "false"
        url = f"/landing/{random.choice(['summer-sale', 'new-arrivals', 'holiday', 'clearance', 'newsletter'])}"
        w.writerow([f"CLICK_{i:07d}", campaign, custkey, clicked.isoformat(), url, converted])

print(f"Wrote 4 seed CSVs to {OUT}")
for csv_file in sorted(OUT.glob("raw_*.csv")):
    rows = sum(1 for _ in csv_file.open()) - 1  # exclude header
    print(f"  {csv_file.name}: {rows:,} rows")
