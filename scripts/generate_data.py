"""
Retail Data Generator
Generates realistic retail transaction data for the data pipeline
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)

class RetailDataGenerator:
    def __init__(self, output_dir='data'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def generate_stores(self, n=50):
        """Generate store data"""
        stores = []
        store_types = ['Supermarket', 'Convenience', 'Hypermarket', 'Express']
        
        for i in range(1, n + 1):
            stores.append({
                'store_id': i,
                'store_name': f"{fake.city()} {random.choice(store_types)}",
                'store_type': random.choice(store_types),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'country': 'USA',
                'opened_date': fake.date_between(start_date='-10y', end_date='-1y'),
                'size_sqft': random.randint(5000, 50000)
            })
        
        df = pd.DataFrame(stores)
        df.to_csv(f'{self.output_dir}/stores.csv', index=False)
        print(f"✓ Generated {len(df)} stores")
        return df
    
    def generate_products(self, n=500):
        """Generate product catalog"""
        categories = {
            'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Camera', 'Smartwatch'],
            'Clothing': ['Shirt', 'Pants', 'Dress', 'Jacket', 'Shoes', 'Accessories'],
            'Food': ['Snacks', 'Beverages', 'Dairy', 'Bakery', 'Frozen', 'Fresh Produce'],
            'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding', 'Bath', 'Garden'],
            'Sports': ['Equipment', 'Apparel', 'Footwear', 'Accessories', 'Outdoor']
        }
        
        products = []
        product_id = 1
        
        for category, subcategories in categories.items():
            for subcategory in subcategories:
                num_products = n // (len(categories) * len(subcategories))
                for _ in range(num_products):
                    base_price = random.uniform(10, 500)
                    products.append({
                        'product_id': product_id,
                        'product_name': f"{fake.word().capitalize()} {subcategory}",
                        'category': category,
                        'subcategory': subcategory,
                        'brand': fake.company(),
                        'cost_price': round(base_price * 0.6, 2),
                        'retail_price': round(base_price, 2),
                        'supplier': fake.company(),
                        'created_date': fake.date_between(start_date='-3y', end_date='today')
                    })
                    product_id += 1
        
        df = pd.DataFrame(products)
        df.to_csv(f'{self.output_dir}/products.csv', index=False)
        print(f"✓ Generated {len(df)} products")
        return df
    
    def generate_customers(self, n=10000):
        """Generate customer data"""
        customers = []
        
        for i in range(1, n + 1):
            signup_date = fake.date_between(start_date='-5y', end_date='today')
            customers.append({
                'customer_id': i,
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.street_address(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'zip_code': fake.zipcode(),
                'signup_date': signup_date,
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
                'loyalty_member': random.choice([True, False])
            })
        
        df = pd.DataFrame(customers)
        df.to_csv(f'{self.output_dir}/customers.csv', index=False)
        print(f"✓ Generated {len(df)} customers")
        return df
    
    def generate_transactions(self, stores_df, products_df, customers_df, days=365):
        """Generate transaction data"""
        transactions = []
        transaction_id = 1
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Generate transactions for each day
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            
            # More transactions on weekends
            is_weekend = current_date.weekday() >= 5
            num_transactions = random.randint(500, 800) if is_weekend else random.randint(300, 500)
            
            for _ in range(num_transactions):
                store_id = random.choice(stores_df['store_id'].tolist())
                customer_id = random.choice(customers_df['customer_id'].tolist())
                
                # Number of items in transaction
                num_items = random.randint(1, 8)
                transaction_items = []
                
                for _ in range(num_items):
                    product = products_df.sample(1).iloc[0]
                    quantity = random.randint(1, 3)
                    
                    # Apply random discount
                    discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
                    unit_price = product['retail_price'] * (1 - discount_pct)
                    
                    transaction_items.append({
                        'transaction_id': transaction_id,
                        'transaction_date': current_date.date(),
                        'transaction_time': f"{random.randint(8, 21):02d}:{random.randint(0, 59):02d}:00",
                        'store_id': store_id,
                        'customer_id': customer_id,
                        'product_id': product['product_id'],
                        'quantity': quantity,
                        'unit_price': round(unit_price, 2),
                        'discount_amount': round(product['retail_price'] * discount_pct * quantity, 2),
                        'total_amount': round(unit_price * quantity, 2),
                        'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'Mobile Payment'])
                    })
                
                transactions.extend(transaction_items)
                transaction_id += 1
        
        df = pd.DataFrame(transactions)
        df.to_csv(f'{self.output_dir}/transactions.csv', index=False)
        print(f"✓ Generated {len(df)} transaction records")
        return df
    
    def generate_all(self):
        """Generate all datasets"""
        print("Starting data generation...")
        print("-" * 50)
        
        stores_df = self.generate_stores(n=50)
        products_df = self.generate_products(n=500)
        customers_df = self.generate_customers(n=10000)
        transactions_df = self.generate_transactions(stores_df, products_df, customers_df, days=365)
        
        print("-" * 50)
        print("✓ All data generated successfully!")
        print(f"Output directory: {self.output_dir}/")
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"  Stores: {len(stores_df):,}")
        print(f"  Products: {len(products_df):,}")
        print(f"  Customers: {len(customers_df):,}")
        print(f"  Transactions: {len(transactions_df):,}")
        print(f"  Total Revenue: ${transactions_df['total_amount'].sum():,.2f}")

if __name__ == "__main__":
    generator = RetailDataGenerator(output_dir='data')
    generator.generate_all()