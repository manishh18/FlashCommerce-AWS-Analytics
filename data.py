import boto3
from dotenv import load_dotenv
import os
import random
import json
import time
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# AWS and Kinesis configuration
REGION = 'us-east-1'
STREAM_NAME = 'IoT'
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize Kinesis client
kinesis = boto3.client(
    'kinesis',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Indian context data
indian_cities = [
    'Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad',
    'Jaipur', 'Lucknow', 'Surat', 'Kanpur', 'Nagpur', 'Indore', 'Thane', 'Bhopal',
    'Visakhapatnam', 'Patna', 'Vadodara', 'Ghaziabad', 'Ludhiana', 'Agra', 'Nashik'
]
product_catalog = [
    {'name': 'Cotton Kurta', 'category': 'Clothing', 'price_range': (499, 1999)},
    {'name': 'Silk Saree', 'category': 'Clothing', 'price_range': (999, 5999)},
    {'name': 'Denim Jeans', 'category': 'Clothing', 'price_range': (799, 2999)},
    {'name': 'Lehenga Choli', 'category': 'Clothing', 'price_range': (1999, 9999)},
    {'name': 'Smartphone', 'category': 'Electronics', 'price_range': (9999, 49999)},
    {'name': 'Bluetooth Earbuds', 'category': 'Electronics', 'price_range': (999, 4999)},
    {'name': 'Laptop', 'category': 'Electronics', 'price_range': (24999, 99999)},
    {'name': 'Smart TV', 'category': 'Electronics', 'price_range': (14999, 79999)},
    {'name': 'Yoga Mat', 'category': 'Fitness', 'price_range': (299, 1499)},
    {'name': 'Dumbbell Set', 'category': 'Fitness', 'price_range': (499, 2999)},
    {'name': 'Treadmill', 'category': 'Fitness', 'price_range': (9999, 39999)},
    {'name': 'Stainless Steel Cookware', 'category': 'Kitchen', 'price_range': (799, 3999)},
    {'name': 'Pressure Cooker', 'category': 'Kitchen', 'price_range': (499, 2499)},
    {'name': 'Non-Stick Tawa', 'category': 'Kitchen', 'price_range': (299, 1499)},
    {'name': 'Mixer Grinder', 'category': 'Kitchen', 'price_range': (1999, 5999)},
    {'name': 'Gold Necklace', 'category': 'Jewelry', 'price_range': (4999, 24999)},
    {'name': 'Silver Anklet', 'category': 'Jewelry', 'price_range': (999, 4999)},
    {'name': 'Diamond Ring', 'category': 'Jewelry', 'price_range': (7999, 39999)},
    {'name': 'Running Shoes', 'category': 'Footwear', 'price_range': (999, 3999)},
    {'name': 'Kolhapuri Chappal', 'category': 'Footwear', 'price_range': (499, 1999)},
    {'name': 'Formal Shoes', 'category': 'Footwear', 'price_range': (1499, 4999)},
    {'name': 'Fiction Novel', 'category': 'Books', 'price_range': (199, 999)},
    {'name': 'Self-Help Book', 'category': 'Books', 'price_range': (149, 799)},
    {'name': 'Wall Art', 'category': 'Home Decor', 'price_range': (499, 2999)},
    {'name': 'Handwoven Rug', 'category': 'Home Decor', 'price_range': (999, 4999)}
]
payment_types = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'Cash on Delivery']
event_types = ['view', 'add_to_cart', 'purchase', 'stock_update', 'price_change']
referral_sources = ['google_ads', 'facebook_ads', 'instagram', 'email_campaign', 'organic_search', 'friend_referral']
age_groups = ['18-24', '25-34', '35-44', '45-54', '55+']
genders = ['female', 'male', 'other']

# Fixed seller IDs (7 unique)
seller_ids = [f"S00{i}" for i in range(1, 8)]  # S001 to S007

# Generate product IDs (25 unique, one per product in catalog)
product_ids = [f"P{i:03d}" for i in range(1, len(product_catalog) + 1)]  # P001 to P025

# Generate user IDs (500 unique customers)
user_ids = [f"U{i:04d}" for i in range(1, 501)]  # U0001 to U0500

def simulate_ecommerce_data():
    """Generate realistic synthetic e-commerce data for Indian context."""
    user_id = random.choice(user_ids)  # Select from 500 users
    session_id = f"S{random.randint(10000, 99999)}"  # Unique session per event
    product_idx = random.randint(0, len(product_catalog) - 1)  # Select product
    product = product_catalog[product_idx]
    product_id = product_ids[product_idx]  # Fixed product ID for this product
    seller_id = random.choice(seller_ids)  # Select from 7 sellers

    # Realistic event type distribution: more views, fewer purchases
    event_weights = {'view': 0.5, 'add_to_cart': 0.2, 'purchase': 0.1, 'stock_update': 0.1, 'price_change': 0.1}
    event_type = random.choices(event_types, weights=[event_weights[e] for e in event_types], k=1)[0]

    # Realistic price and cost price
    price = round(random.uniform(product['price_range'][0], product['price_range'][1]), 2)
    cost_price = round(price * random.uniform(0.6, 0.8), 2)  # 60-80% of price for margin

    # Realistic discount
    discount_percentage = random.randint(0, 30) if event_type != 'price_change' else random.randint(5, 50)

    # Quantity for purchases (1-3), 0 for other events
    quantity = random.randint(1, 3) if event_type == 'purchase' else 0

    # Stock quantity: reduce by quantity for purchases
    base_stock = random.randint(10, 100)
    stock_quantity = max(1, base_stock - quantity) if event_type == 'purchase' else base_stock

    # Payment type: weighted for Indian preferences, only for purchases
    payment_weights = {'UPI': 0.4, 'Cash on Delivery': 0.3, 'Credit Card': 0.15, 'Debit Card': 0.1, 'Net Banking': 0.05}
    payment_type = random.choices(
        payment_types,
        weights=[payment_weights[p] for p in payment_types],
        k=1
    )[0] if event_type == 'purchase' else ''

    # Rating: 1.0-5.0, skewed toward positive (80% 3.0-5.0, 20% 1.0-2.9)
    rating = round(random.uniform(3.0, 5.0) if random.random() < 0.8 else random.uniform(1.0, 2.9), 1)

    return {
        "event_time": datetime.utcnow().isoformat() + 'Z',
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "device_type": random.choice(['mobile', 'desktop', 'tablet']),
        "referral_source": random.choice(referral_sources),
        "campaign_id": f"C{random.randint(100, 999)}",
        "product_id": product_id,
        "product_name": product['name'],
        "category": product['category'],
        "price": price,
        "cost_price": cost_price,
        "discount_percentage": discount_percentage,
        "quantity": quantity,
        "stock_quantity": stock_quantity,
        "seller_id": seller_id,
        "city": random.choice(indian_cities),
        "payment_type": payment_type,
        "customer_age_group": random.choice(age_groups),
        "customer_gender": random.choice(genders),
        "rating": rating,
        "promotion_active": random.choice([True, False])
    }

def send_to_kinesis(data):
    """Send data to Kinesis stream."""
    try:
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data) + '\n',  # Add newline for JSONL
            PartitionKey=data['user_id']
        )
        print(f"Record sent: {data['product_id']} by {data['user_id']}, SequenceNumber: {response['SequenceNumber']}")
        return True
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")
        return False

def main():
    """Generate and stream 100 realistic e-commerce records."""
    count = 0
    max_records = 100
    try:
        while count < max_records:
            data = simulate_ecommerce_data()
            print(f"Sending record {count + 1}/{max_records}: {data['event_type']} for {data['product_name']} (PID: {data['product_id']})")
            if send_to_kinesis(data):
                count += 1
            time.sleep(1)  # 1-second delay
        print(f"Completed sending {count} records.")
    except KeyboardInterrupt:
        print(f"\nStopped after sending {count} records.")

if __name__ == "__main__":
    main()
