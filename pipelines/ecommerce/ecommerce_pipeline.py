"""
E-commerce Pipeline
Simulates a realistic online store with orders, customers and products.
Domain: retail analytics
"""

import dlt
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import random

# --- paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH  = PROJECT_ROOT / "data" / "processed" / "ecommerce.duckdb"

# --- config ---
DAYS_OF_DATA  = 90   # 3 months of sales
NUM_CUSTOMERS = 500
NUM_PRODUCTS  = 50

COUNTRIES = ["Germany", "France", "Netherlands", "Poland", "Romania", "Sweden"]

CATEGORIES = {
    "Electronics":  {"brands": ["Sony", "Samsung", "Apple"],     "price_range": (50,  1500)},
    "Clothing":     {"brands": ["Zara", "H&M", "Nike"],          "price_range": (10,  200)},
    "Home & Garden":{"brands": ["IKEA", "Bosch", "Philips"],     "price_range": (15,  500)},
    "Sports":       {"brands": ["Adidas", "Nike", "Decathlon"],  "price_range": (20,  400)},
    "Books":        {"brands": ["Penguin", "OReilly", "Packt"],  "price_range": (8,   60)},
}

ORDER_STATUSES    = ["completed", "returned", "cancelled"]
ORDER_STATUS_WEIGHTS = [60, 20, 20]
# weighted — most orders complete, some return, few cancel

# --- helpers ---
def generate_products():
    """Generate a realistic product catalog."""
    products = []
    product_id = 1

    for category, details in CATEGORIES.items():
        for brand in details["brands"]:
            for _ in range(3):  # 3 products per brand
                price = round(random.uniform(*details["price_range"]), 2)
                cost  = round(price * random.uniform(0.4, 0.7), 2)  # 30-60% margin

                products.append({
                    "product_id":   f"PROD_{product_id:03d}",
                    "name":         f"{brand} {category} #{product_id}",
                    "category":     category,
                    "brand":        brand,
                    "price_eur":    price,
                    "cost_eur":     cost,
                    "margin_pct":   round((price - cost) / price * 100, 1),
                })
                product_id += 1

    return products


def generate_customers():
    """Generate a customer base with segments."""
    customers = []

    for i in range(1, NUM_CUSTOMERS + 1):
        country = random.choice(COUNTRIES)

        # VIP customers spend more and order more often
        segment = random.choices(
            ["vip", "regular", "occasional"],
            weights=[10, 50, 40]
        )[0]

        customers.append({
            "customer_id":      f"CUST_{i:04d}",
            "country":          country,
            "segment":          segment,
            "registration_date": (
                datetime.now() - timedelta(days=random.randint(30, 730))
            ).date().isoformat(),
        })

    return customers

# --- resources ---
@dlt.resource(name="products", write_disposition="replace")
def products_resource():
    """Product catalog — static, replaced on each run."""
    for product in generate_products():
        yield product


@dlt.resource(name="customers", write_disposition="replace")
def customers_resource():
    """Customer base — static, replaced on each run."""
    for customer in generate_customers():
        yield customer


@dlt.resource(name="orders", write_disposition="replace")
def orders_resource():
    """
    Generate 3 months of orders.
    Order volume varies by day — weekends and end of month are busier.
    """
    products  = generate_products()
    customers = generate_customers()
    start_date = datetime.now() - timedelta(days=DAYS_OF_DATA)
    order_id   = 1

    for day in range(DAYS_OF_DATA):
        order_date = start_date + timedelta(days=day)
        is_weekend = order_date.weekday() >= 5

        # More orders on weekends and end of month
        is_end_of_month = order_date.day >= 25
        daily_orders = random.randint(
            20 if is_weekend or is_end_of_month else 10,
            50 if is_weekend or is_end_of_month else 30,
        )

        for _ in range(daily_orders):
            customer = random.choice(customers)
            product  = random.choice(products)

            # VIP customers buy more items per order
            quantity = random.choices(
                [1, 2, 3, 4, 5],
                weights=[40, 30, 15, 10, 5]
                if customer["segment"] != "vip"
                else [10, 20, 30, 25, 15]
            )[0]

            status        = random.choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)[0]
            total_eur     = round(product["price_eur"] * quantity, 2)
            total_cost    = round(product["cost_eur"]  * quantity, 2)

            yield {
                "order_id":       f"ORD_{order_id:06d}",
                "customer_id":    customer["customer_id"],
                "product_id":     product["product_id"],
                "country":        customer["country"],
                "segment":        customer["segment"],
                "category":       product["category"],
                "brand":          product["brand"],
                "order_date":     order_date.date().isoformat(),
                "status":         status,
                "quantity":       quantity,
                "unit_price_eur": product["price_eur"],
                "total_eur":      total_eur,
                "total_cost_eur": total_cost,
                "margin_eur":     round(total_eur - total_cost, 2),
                "is_weekend":     is_weekend,
            }
            order_id += 1

            # --- source ---
@dlt.source
def ecommerce_source():
    return [products_resource(), customers_resource(), orders_resource()]


# --- pipeline ---
def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="ecommerce_pipeline",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_ecommerce",
    )

    print("🛒 Running e-commerce pipeline...")
    load_info = pipeline.run(ecommerce_source())
    print(f"✅ Done: {load_info}")

    # --- sanity check ---
    conn = duckdb.connect(str(DUCKDB_PATH))

    orders_count   = conn.execute("SELECT COUNT(*) FROM raw_ecommerce.orders").fetchone()[0]
    products_count = conn.execute("SELECT COUNT(*) FROM raw_ecommerce.products").fetchone()[0]
    customers_count= conn.execute("SELECT COUNT(*) FROM raw_ecommerce.customers").fetchone()[0]

    print(f"\n📦 Products:  {products_count}")
    print(f"👥 Customers: {customers_count}")
    print(f"🧾 Orders:    {orders_count:,}")

    print("\n📊 Revenue by category:")
    print(conn.execute("""
        SELECT
            category,
            COUNT(*)                    AS orders,
            ROUND(SUM(total_eur), 2)    AS revenue_eur,
            ROUND(SUM(margin_eur), 2)   AS profit_eur
        FROM raw_ecommerce.orders
        WHERE status = 'completed'
        GROUP BY category
        ORDER BY revenue_eur DESC
    """).df().to_string())

    conn.close()


if __name__ == "__main__":
    run_pipeline()