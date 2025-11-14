#!/usr/bin/env python3
"""
Generate synthetic e-commerce CSV datasets.
Uses Faker library with seed=42 for reproducibility.
"""

import os
import csv
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

try:
    from faker import Faker
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False
    print("# Warning: Faker not installed. Using built-in random data generation.")

# Set seeds for reproducibility
random.seed(42)
if FAKER_AVAILABLE:
    fake = Faker()
    Faker.seed_instance(42)
else:
    fake = None

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Categories for products
CATEGORIES = ["electronics", "home", "beauty", "books", "clothing", "sports", "toys", "automotive"]
ORDER_STATUSES = ["pending", "paid", "shipped", "cancelled", "returned"]

# Helper functions for data generation
def generate_email(name):
    """Generate email from name."""
    if fake:
        return fake.email()
    name_clean = name.lower().replace(' ', '.')
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com']
    return f"{name_clean}@{random.choice(domains)}"

def generate_name():
    """Generate customer name."""
    if fake:
        return fake.name()
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'James', 'Emma']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def generate_country():
    """Generate country name."""
    if fake:
        return fake.country()
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan', 'India']
    return random.choice(countries)

def generate_text(min_words=5, max_words=20):
    """Generate review text."""
    if fake:
        return fake.text(max_nb_chars=200)
    words = ['great', 'good', 'excellent', 'amazing', 'love', 'nice', 'quality', 'fast', 'recommend']
    num_words = random.randint(min_words, max_words)
    return ' '.join(random.choices(words, k=num_words)).capitalize() + '.'

def generate_product_name(category):
    """Generate product name for category."""
    if fake:
        return fake.catch_phrase()
    prefixes = {'electronics': ['Smart', 'Digital', 'Wireless', 'Pro'],
                'home': ['Premium', 'Classic', 'Modern', 'Elegant'],
                'beauty': ['Luxury', 'Natural', 'Organic', 'Professional'],
                'books': ['The', 'Complete Guide to', 'Advanced', 'Introduction to'],
                'clothing': ['Designer', 'Classic', 'Sport', 'Casual'],
                'sports': ['Professional', 'Elite', 'Training', 'Competition'],
                'toys': ['Fun', 'Educational', 'Interactive', 'Creative'],
                'automotive': ['Heavy Duty', 'Premium', 'Performance', 'Classic']}
    suffix = category.title()
    return f"{random.choice(prefixes.get(category, ['Premium']))} {suffix} Item {random.randint(1, 1000)}"


# Generate customers.csv
print("Generating customers.csv...")
customers = []
for i in range(500):
    customer_id = str(uuid.uuid4())
    name = generate_name()
    email = generate_email(name)
    signup_date = (datetime.now() - timedelta(days=random.randint(0, 1095))).isoformat()
    country = generate_country()
    is_premium = random.choice([True, False, False, False])  # 25% premium
    
    customers.append({
        'customer_id': customer_id,
        'name': name,
        'email': email,
        'signup_date': signup_date,
        'country': country,
        'is_premium': str(is_premium)
    })

with open('data/customers.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['customer_id', 'name', 'email', 'signup_date', 'country', 'is_premium'])
    writer.writeheader()
    writer.writerows(customers)

print(f"  Generated {len(customers)} customers")


# Generate products.csv
print("Generating products.csv...")
products = []
for i in range(200):
    product_id = i + 1
    # Generate unique SKU using product_id to ensure uniqueness
    random_num = random.randint(10000, 99999)
    suffix = random.choice(['A', 'B', 'C'])
    sku = f"SKU-{random_num:05d}-{suffix}-{product_id:04d}"
    category = random.choice(CATEGORIES)
    name = generate_product_name(category)
    price = Decimal(random.uniform(9.99, 999.99)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    cost = Decimal(float(price) * random.uniform(0.3, 0.7)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    created_at = (datetime.now() - timedelta(days=random.randint(0, 730))).isoformat()
    
    products.append({
        'product_id': product_id,
        'sku': sku,
        'name': name,
        'category': category,
        'price': str(price),
        'cost': str(cost),
        'created_at': created_at
    })

with open('data/products.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['product_id', 'sku', 'name', 'category', 'price', 'cost', 'created_at'])
    writer.writeheader()
    writer.writerows(products)

print(f"  Generated {len(products)} products")


# Generate orders.csv
print("Generating orders.csv...")
orders = []
customer_ids = [c['customer_id'] for c in customers]
product_ids = [p['product_id'] for p in products]

for i in range(1500):
    order_id = i + 1
    customer_id = random.choice(customer_ids)
    order_date = (datetime.now() - timedelta(days=random.randint(0, 730))).isoformat()
    status = random.choices(ORDER_STATUSES, weights=[5, 60, 25, 5, 5])[0]  # Most are paid/shipped
    shipping_country = random.choice([c['country'] for c in customers if c['customer_id'] == customer_id])
    
    # Total will be calculated from order_items, so we'll set a placeholder
    total_amount = Decimal('0.00')
    
    orders.append({
        'order_id': order_id,
        'customer_id': customer_id,
        'order_date': order_date,
        'status': status,
        'total_amount': str(total_amount),
        'shipping_country': shipping_country
    })

with open('data/orders.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'shipping_country'])
    writer.writeheader()
    writer.writerows(orders)

print(f"  Generated {len(orders)} orders")


# Generate order_items.csv and update orders with correct totals
print("Generating order_items.csv...")
order_items = []
order_totals = {}
order_item_id_counter = 1

for order in orders:
    order_id = order['order_id']
    num_items = random.randint(1, 6)
    items_for_order = []
    
    # Select unique products for this order
    selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
    
    for idx, product_id in enumerate(selected_products):
        order_item_id = order_item_id_counter
        order_item_id_counter += 1
        product = next(p for p in products if p['product_id'] == product_id)
        quantity = random.randint(1, 5)
        unit_price = Decimal(product['price'])
        
        items_for_order.append({
            'order_item_id': order_item_id,
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': str(unit_price)
        })
        
        # Calculate total for this order
        if order_id not in order_totals:
            order_totals[order_id] = Decimal('0.00')
        order_totals[order_id] += unit_price * quantity
    
    order_items.extend(items_for_order)

# Update orders with correct totals
for order in orders:
    order_id = order['order_id']
    if order_id in order_totals:
        order['total_amount'] = str(order_totals[order_id].quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

# Write order_items
with open('data/order_items.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price'])
    writer.writeheader()
    writer.writerows(order_items)

# Rewrite orders with updated totals
with open('data/orders.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'shipping_country'])
    writer.writeheader()
    writer.writerows(orders)

print(f"  Generated {len(order_items)} order items")


# Generate reviews.csv
print("Generating reviews.csv...")
reviews = []
# Not every customer reviews, and not every product gets reviewed
reviewing_customers = random.sample(customer_ids, k=min(400, len(customer_ids)))
reviewed_products = random.sample(product_ids, k=min(150, len(product_ids)))

for i in range(800):
    review_id = i + 1
    product_id = random.choice(reviewed_products)
    customer_id = random.choice(reviewing_customers)
    # Skewed positive: 40% 5-star, 30% 4-star, 15% 3-star, 10% 2-star, 5% 1-star
    rating = random.choices([5, 4, 3, 2, 1], weights=[40, 30, 15, 10, 5])[0]
    review_text = generate_text()
    created_at = (datetime.now() - timedelta(days=random.randint(0, 600))).isoformat()
    
    reviews.append({
        'review_id': review_id,
        'product_id': product_id,
        'customer_id': customer_id,
        'rating': rating,
        'review_text': review_text,
        'created_at': created_at
    })

with open('data/reviews.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['review_id', 'product_id', 'customer_id', 'rating', 'review_text', 'created_at'])
    writer.writeheader()
    writer.writerows(reviews)

print(f"  Generated {len(reviews)} reviews")


# Validation: Check foreign key consistency
print("\nValidating foreign key consistency...")
customer_id_set = set(c['customer_id'] for c in customers)
product_id_set = set(p['product_id'] for p in products)
order_id_set = set(o['order_id'] for o in orders)

# Validate orders reference valid customers
invalid_customer_refs = sum(1 for o in orders if o['customer_id'] not in customer_id_set)
if invalid_customer_refs > 0:
    print(f"  WARNING: {invalid_customer_refs} orders reference invalid customers")

# Validate order_items reference valid orders and products
invalid_order_refs = sum(1 for oi in order_items if oi['order_id'] not in order_id_set)
invalid_product_refs = sum(1 for oi in order_items if oi['product_id'] not in product_id_set)
if invalid_order_refs > 0:
    print(f"  WARNING: {invalid_order_refs} order_items reference invalid orders")
if invalid_product_refs > 0:
    print(f"  WARNING: {invalid_product_refs} order_items reference invalid products")

# Validate reviews reference valid products and customers
invalid_review_product_refs = sum(1 for r in reviews if r['product_id'] not in product_id_set)
invalid_review_customer_refs = sum(1 for r in reviews if r['customer_id'] not in customer_id_set)
if invalid_review_product_refs > 0:
    print(f"  WARNING: {invalid_review_product_refs} reviews reference invalid products")
if invalid_review_customer_refs > 0:
    print(f"  WARNING: {invalid_review_customer_refs} reviews reference invalid customers")

# Validate order totals
mismatches = 0
for order in orders:
    order_id = order['order_id']
    expected_total = sum(
        Decimal(oi['quantity']) * Decimal(oi['unit_price'])
        for oi in order_items
        if oi['order_id'] == order_id
    ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    actual_total = Decimal(order['total_amount'])
    if abs(expected_total - actual_total) > Decimal('0.01'):
        mismatches += 1

if mismatches > 0:
    print(f"  WARNING: {mismatches} orders have mismatched totals")
else:
    print("  All order totals match order_items")

# Summary
print("\n=== Generation Summary ===")
print(f"Customers: {len(customers)}")
print(f"Products: {len(products)}")
print(f"Orders: {len(orders)}")
print(f"Order Items: {len(order_items)}")
print(f"Reviews: {len(reviews)}")

# Top 3 categories by product count
category_counts = {}
for p in products:
    category_counts[p['category']] = category_counts.get(p['category'], 0) + 1
top_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:3]
print("\nTop 3 categories by product count:")
for category, count in top_categories:
    print(f"  {category}: {count} products")

print("\nData generation complete! Files written to data/")

