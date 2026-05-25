import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from pathlib import Path

BRONZE_DIR = Path(__file__).resolve().parent / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# Initialize Faker for realistic fake names
fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

print("Starting synthetic data generation...")

# ---------------------------------------------------------
# 1. GENERATE ANONYMIZED PARTNERS (DIMENSION)
# ---------------------------------------------------------
partners_pool = [
    {"partner_id": 101, "partner_name": "Apex Supply Co", "partner_type": "National"},
    {"partner_id": 102, "partner_name": "Matrix Parts Group", "partner_type": "Regional"},
    {"partner_id": 103, "partner_name": "Vertex Finishings", "partner_type": "National"},
    {"partner_id": 104, "partner_name": "Beacon Materials", "partner_type": "Specialty"},
    {"partner_id": 105, "partner_name": "Titan Logistics", "partner_type": "Regional"},
]
partners_df = pd.DataFrame(partners_pool)

# ---------------------------------------------------------
# 2. GENERATE ANONYMIZED AFFILIATES (DIMENSION)
# ---------------------------------------------------------
affiliate_records = []
states = ['GA', 'FL', 'NC', 'SC', 'TN', 'AL']

# Create a few fake Multi-Shop Organizations (MSOs) / Parent IDs
parent_ids = [None, 5001, 5002, 5003]

for i in range(1, 151):  # 150 fake shops
    aff_id = 1000 + i
    p_id = random.choice(parent_ids) if i > 20 else None # Mix of MSO and independents
    
    affiliate_records.append({
        "affiliate_id": aff_id,
        "affiliate_name": f"{fake.city()} Auto Body" if random.random() > 0.3 else f"{fake.last_name()} Collision Center",
        "parent_id": p_id,
        "state": random.choice(states),
        "sponsor_tier": random.randint(1, 8)  # Replaces the proprietary "paint_sponsor" column name
    })

affiliates_df = pd.DataFrame(affiliate_records)

# ---------------------------------------------------------
# 3. GENERATE REBATE TRANSACTIONS (FACT WITH INJECTED ANOMALIES)
# ---------------------------------------------------------
tx_records = []
start_date = datetime(2024, 1, 1)

# Separate active affiliates from the "Silent" ones to simulate the anomaly
all_affiliate_ids = affiliates_df["affiliate_id"].tolist()
# Randomly select a large chunk to be completely silent (e.g., 60 silent shops out of 150)
silent_affiliate_ids = set(random.sample(all_affiliate_ids, k=60))
active_affiliate_ids = [aid for aid in all_affiliate_ids if aid not in silent_affiliate_ids]

print(f"Generated {len(silent_affiliate_ids)} completely silent shops for anomaly detection verification.")

# Generate 5,000 baseline transaction grains
for tx_idx in range(100001, 105001):
    # Core variables
    tx_id = f"TXN-{tx_idx}"
    aff_id = random.choice(active_affiliate_ids)
    partner = random.choice(partners_pool)
    
    # Generate dates spanning 2024 to 2026
    days_to_add = random.randint(0, 850)
    tx_date = start_date + timedelta(days=days_to_add)
    
    # Base amounts
    base_amount = round(random.uniform(50.0, 12000.0), 2)
    
    # INJECT ANOMALY: Rebate Decomposition (Same Tx ID, multiple rows for program tiers)
    # Simulate a base rebate and a promotional bonus rebate for the same transaction
    num_programs = 1
    if random.random() > 0.85:  # 15% of transactions trigger dual rebate programs
        num_programs = 2
        
    for p_idx in range(num_programs):
        program_memo = "Base Program Rebate" if p_idx == 0 else "Q4 Promotional Tier Bonus"
        rebate_rate = 0.05 if p_idx == 0 else 0.02
        
        # INJECT ANOMALY: Floating-Point/Rounding Errors
        # Some rows get clean rounding, others get raw floats to force the Silver layer to fix it
        net_amount = base_amount * rebate_rate
        if random.random() > 0.95:
            # Leave unrounded float noise
            net_amount = net_amount + 0.00012345
        else:
            net_amount = round(net_amount, 2)
            
        # INJECT ANOMALY: Negative Amounts (Returns / Adjustments)
        if random.random() > 0.98:  # 2% of data are negative adjustments
            net_amount = -abs(net_amount)
            program_memo = "Return Credit / Adjustment Note"

        tx_records.append({
            "transaction_id": tx_id,
            "affiliate_id": aff_id,
            "partner_id": partner["partner_id"],
            "transaction_date": tx_date.strftime("%Y-%m-%d"),
            "memo": program_memo,
            "net_amount": net_amount
        })

transactions_df = pd.DataFrame(tx_records)

# ---------------------------------------------------------
# 4. EXPORT SAFE, SYNTHETIC CSV FILES
# ---------------------------------------------------------
affiliates_df.to_csv(BRONZE_DIR / "synthetic_dim_affiliate.csv", index=False)
partners_df.to_csv(BRONZE_DIR / "synthetic_dim_partner.csv", index=False)
transactions_df.to_csv(BRONZE_DIR / "synthetic_fact_rebate.csv", index=False)

print("SUCCESS: Synthetic dataset created perfectly!")
print(f"-> Affiliates Shape: {affiliates_df.shape}")
print(f"-> Partners Shape: {partners_df.shape}")
print(f"-> Transactions Shape: {transactions_df.shape}")