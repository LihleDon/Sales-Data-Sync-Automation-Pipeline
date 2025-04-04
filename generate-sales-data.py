import pandas as pd
import numpy as np

# Seed for reproducibility
np.random.seed(42)

# Generate sales1.csv (5 records)
sales1_data = {
    "id": [1, 2, 3, 4, 5],
    "amount": [100.50, 200.75, 150.00, 300.25, 50.80]
}
sales1_df = pd.DataFrame(sales1_data)
sales1_df.to_csv("sales1.csv", index=False)

# Generate sales2.csv (5 records, some matching IDs)
sales2_data = {
    "id": [1, 2, 6, 7, 8],
    "amount": [120.00, 180.90, 90.10, 250.00, 75.30]
}
sales2_df = pd.DataFrame(sales2_data)
sales2_df.to_csv("sales2.csv", index=False)

print("Generated sales1.csv and sales2.csv")