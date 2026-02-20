import pandas as pd
import random
from datetime import datetime, timedelta

N = 50000
regions = ["North", "South", "East", "West", "Central"]

data = []
start = datetime(2024, 1, 1)

for i in range(1, N + 1):
    signup = start + timedelta(days=random.randint(0, 700))
    data.append([
        i,
        f"Customer{i}",
        random.choice(regions),
        signup.strftime("%Y-%m-%d"),
        signup.strftime("%Y-%m-%d")
    ])

df = pd.DataFrame(data, columns=[
    "customer_id",
    "name",
    "region",
    "signup_date",
    "updated_at"
])

updates = df.sample(int(N * 0.1), random_state=42)

update_records = []

for _, row in updates.iterrows():
    change_date = datetime(2026, 1, 1)
    update_records.append([
        row["customer_id"],
        row["name"],
        random.choice(regions),
        row["signup_date"],
        change_date.strftime("%Y-%m-%d")
    ])

update_df = pd.DataFrame(update_records, columns=df.columns)

df = pd.concat([df, update_df], ignore_index=True)

df.to_csv("data/raw/customers.csv", index=False)

print("Generated 50K customers with simulated updates.")