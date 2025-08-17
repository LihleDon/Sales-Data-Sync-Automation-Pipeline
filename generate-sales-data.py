import pandas as pd
import numpy as np
import os
import urllib.request

# Seed for reproducibility
np.random.seed(42)

# Generate small mock datasets
def generate_mock_data():
    sales1_data = {
        "id": [1, 2, 3, 4, 5],
        "amount": [100.50, 200.75, 150.00, 300.25, 50.80]
    }
    sales1_df = pd.DataFrame(sales1_data)
    sales1_df.to_csv("sales1.csv", index=False)

    sales2_data = {
        "id": [1, 2, 6, 7, 8],
        "amount": [120.00, 180.90, 90.10, 250.00, 75.30]
    }
    sales2_df = pd.DataFrame(sales2_data)
    sales2_df.to_csv("sales2.csv", index=False)

# Download and preprocess Kaggle Online Retail Dataset
def generate_kaggle_data():
    url = "https://raw.githubusercontent.com/vijayuv/onlineretail/master/Online%20Retail.csv"
    file_path = "online_retail.csv"
    
    # Download dataset
    if not os.path.exists(file_path):
        urllib.request.urlretrieve(url, file_path)
    
    # Load and preprocess
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    # Clean data: remove missing values and negative amounts
    df = df.dropna(subset=['InvoiceNo', 'Quantity', 'UnitPrice'])
    df = df[df['Quantity'] > 0]
    df['amount'] = df['Quantity'] * df['UnitPrice']
    
    # Select relevant columns and rename for compatibility
    processed_df = df[['InvoiceNo', 'amount']].rename(columns={'InvoiceNo': 'id'})
    processed_df['id'] = processed_df['id'].astype(str)  # Treat InvoiceNo as string ID
    
    # Sample 10,000 records for testing
    processed_df = processed_df.sample(n=10000, random_state=42)
    processed_df.to_csv("sales_kaggle.csv", index=False)

if __name__ == "__main__":
    generate_mock_data()
    generate_kaggle_data()
    print("Generated sales1.csv, sales2.csv, and sales_kaggle.csv")
