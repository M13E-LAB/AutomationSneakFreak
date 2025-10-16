# === IMPORTS ===
import datetime  # For timestamp generation
import random    # For random data generation

import pandas as pd    # For DataFrame operations
from faker import Faker  # For realistic fake data generation


def gaussian_clamped(rng: random.Random, mu: float, sigma: float, a: float, b: float) -> float:
    """Generate a Gaussian random number clamped between bounds.
    
    Uses Box-Muller transformation with clamping to ensure values stay within [a, b].
    Useful for generating realistic distributions with hard limits.
    
    Args:
        rng: Random number generator instance
        mu: Mean of the Gaussian distribution
        sigma: Standard deviation
        a: Minimum bound (inclusive)
        b: Maximum bound (inclusive)
        
    Returns:
        float: Random value between a and b following Gaussian distribution
    """
    # Box-Muller transformation with clamp
    val = rng.gauss(mu, sigma)
    return max(a, min(b, val))


def generate_orders(
    orders: int,
    seed: int = 42,
    start: str = "2024-01-01",
    end: str = "2025-09-01",
    inventory: pd.DataFrame = None,
    customers: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Generate realistic order data for e-commerce simulation.

    Creates orders with realistic quantity distributions, timestamps,
    and references to existing customers and products when provided.

    Args:
        orders: Number of orders to generate
        seed: Random seed for reproducibility (default: 42)
        start: Start date for order timestamps (YYYY-MM-DD format)
        end: End date for order timestamps (YYYY-MM-DD format)
        inventory: DataFrame with product data (improves realism with real prices)
        customers: DataFrame with customer data (improves realism with real IDs)

    Returns:
        pd.DataFrame: DataFrame with columns [id, product_id, customer_id, quantity, unit_price, sold_at]
    """
    # === RANDOM GENERATOR SETUP ===
    rng = random.Random(seed)  # Deterministic random generator for reproducibility

    # === DATE RANGE CALCULATION ===
    # Convert string dates to UTC datetime objects
    start_dt = datetime.datetime.fromisoformat(start).replace(tzinfo=datetime.timezone.utc)
    end_dt = datetime.datetime.fromisoformat(end).replace(tzinfo=datetime.timezone.utc)
    delta_seconds = int((end_dt - start_dt).total_seconds())  # Total seconds in date range

    # === REFERENCE DATA SETUP ===
    # Default product IDs if no inventory provided
    product_ids = list(range(1000, 1250))  # Default range: 1000-1249
    prices = {}  # Price lookup dictionary
    
    # Use real inventory data if provided (more realistic)
    if inventory is not None and not inventory.empty:
        product_ids = inventory["product_id"].tolist()  # Use actual product IDs
        # Create price lookup for realistic pricing
        # Python 3.9 compatibility: zip has no 'strict' kwarg
        prices = dict(zip(inventory["product_id"], inventory["unit_price"]))

    # Default customer IDs if no customers provided
    customer_ids = list(range(1, 1001))  # Default range: 1-1000
    if customers is not None and not customers.empty:
        customer_ids = customers["customer_id"].tolist()  # Use actual customer IDs

    # === ORDER GENERATION ===
    rows = []
    for i in range(1, orders + 1):
        # === PRODUCT SELECTION ===
        pid = rng.choice(product_ids)  # Random product from available inventory
        
        # === QUANTITY DISTRIBUTION ===
        # Realistic quantity distribution: most people buy 1-2 items
        # 60% buy 1, 20% buy 2, 12% buy 3, 6% buy 4, 2% buy 5
        qty = rng.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.12, 0.06, 0.02])[0]
        
        # === TIMESTAMP GENERATION ===
        # Random timestamp within the specified date range
        sold_at = start_dt + datetime.timedelta(seconds=rng.randint(0, delta_seconds))
        
        # === CUSTOMER SELECTION ===
        cid = rng.choice(customer_ids)  # Random customer from available list
        
        # === PRICE DETERMINATION ===
        # Use real price from inventory if available, otherwise generate random price
        unit_price = prices.get(pid, round(rng.uniform(5, 500), 2))
        
        # === ORDER RECORD CREATION ===
        rows.append(
            {
                "id": i,                # Sequential order ID
                "product_id": pid,     # Reference to product
                "customer_id": cid,    # Reference to customer
                "quantity": qty,       # Number of items ordered
                "unit_price": unit_price,  # Price per unit
                "sold_at": sold_at,    # Sale timestamp
            }
        )

    # === DATAFRAME CREATION ===
    df = pd.DataFrame(rows)

    # === DATA TYPE CONVERSION ===
    # Ensure SOLD_AT is properly formatted as datetime for database compatibility
    df["sold_at"] = pd.to_datetime(df["sold_at"])

    print(f"✅ Generated {len(df)} orders as DataFrame")
    return df


# === SNEAKER REFERENCE DATA ===
# Real sneaker brands and their popular models for realistic data generation
SNEAKER_BRANDS = [
    ("Nike", ["Air Max", "Air Force 1", "Dunk", "Jordan", "React", "Zoom", "Blazer", "Cortez"]),
    ("Adidas", ["Stan Smith", "Superstar", "Gazelle", "Ultra Boost", "NMD", "Samba", "Campus", "Yeezy"]),
    ("New Balance", ["990", "574", "327", "2002R", "550", "1906R", "9060", "Fresh Foam"]),
    ("Converse", ["Chuck Taylor", "One Star", "Pro Leather", "Jack Purcell", "Run Star Hike"]),
    ("Vans", ["Old Skool", "Authentic", "Era", "Sk8-Hi", "Slip-On", "Knu Skool"]),
    ("Puma", ["Suede", "Clyde", "RS-X", "Speedcat", "Palermo", "Easy Rider"]),
    ("Asics", ["Gel-Lyte III", "Gel-Kayano", "Japan S", "Mexico 66", "Gel-Nimbus"]),
    ("Reebok", ["Club C", "Classic Leather", "Pump", "Instapump Fury", "Question"]),
]

# Common sneaker attributes for realistic product generation
SNEAKER_COLORS = ["White", "Black", "Grey", "Navy", "Red", "Blue", "Green", "Brown", "Cream", "Pink", "Purple", "Orange"]
SNEAKER_MATERIALS = ["Leather", "Canvas", "Suede", "Mesh", "Knit", "Synthetic"]
SNEAKER_EDITIONS = ["", "Retro", "OG", "Low", "Mid", "High", "Premium", "Limited", "Special Edition"]


def generate_inventory_data(products: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate sneaker inventory data and return as a pandas DataFrame.

    Args:
        products: Number of sneaker products to generate
        seed: Random seed for reproducibility

    Returns:
        pandas.DataFrame: DataFrame containing sneaker inventory data
    """
    rng = random.Random(seed)

    rows = []
    product_ids = list(range(1000, 1000 + products))
    
    # Tailles de sneakers disponibles
    sizes = ["36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46"]
    
    for pid in product_ids:
        brand, models = rng.choice(SNEAKER_BRANDS)
        model = rng.choice(models)
        color = rng.choice(SNEAKER_COLORS)
        material = rng.choice(SNEAKER_MATERIALS)
        edition = rng.choice(SNEAKER_EDITIONS)
        size = rng.choice(sizes)
        
        # Construire le nom du produit
        if edition and edition != "":
            product_name = f"{brand} {model} {edition} {color} {material} - Size {size}"
        else:
            product_name = f"{brand} {model} {color} {material} - Size {size}"
        
        # Prix basés sur la marque avec variance réaliste
        brand_prices = {
            "Nike": 120, "Adidas": 110, "New Balance": 130, "Converse": 70,
            "Vans": 65, "Puma": 80, "Asics": 100, "Reebok": 85
        }
        base_price = brand_prices.get(brand, 90)
        
        # Ajustement prix selon l'édition
        if "Limited" in edition or "Special Edition" in edition:
            base_price *= 1.5
        elif "Premium" in edition:
            base_price *= 1.2
        elif "Retro" in edition or "OG" in edition:
            base_price *= 1.1
            
        price = round(gaussian_clamped(rng, base_price, base_price * 0.3, base_price * 0.5, base_price * 2.0), 2)
        
        # Stock : tailles populaires ont plus de stock
        popular_sizes = ["40", "41", "42", "43"]
        if size in popular_sizes:
            stock_qty = int(gaussian_clamped(rng, 50, 30, 5, 200))
        else:
            stock_qty = int(gaussian_clamped(rng, 25, 20, 0, 100))
        
        rows.append(
            {
                "product_id": pid,
                "product_name": product_name,
                "brand": brand,
                "model": model,
                "color": color,
                "material": material,
                "size": size,
                "edition": edition if edition else "Standard",
                "category": "Sneakers",
                "unit_price": price,
                "stock_quantity": stock_qty,
            }
        )

    df = pd.DataFrame(rows)
    print(f"✅ Generated {len(df)} sneaker inventory data as DataFrame")
    return df


# -- Generate customers --


def generate_customers(customers: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate customer data and return as a pandas DataFrame.

    Args:
        customers: Number of customers to generate
        seed: Random seed for reproducibility

    Returns:
        pandas.DataFrame: DataFrame containing customer data
    """
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    rows = []
    channels = [("online", 0.65), ("store", 0.35)]

    for cid in range(1, customers + 1):
        name = fake.name()
        email = fake.email()
        city = fake.city()
        channel = rng.choices([c for c, _ in channels], weights=[w for _, w in channels])[0]
        rows.append(
            {
                "customer_id": cid,
                "name": name,
                "email": email,
                "city": city,
                "channel": channel,
            }
        )

    df = pd.DataFrame(rows)
    print(f"✅ Generated {len(df)} customers as DataFrame")
    return df


if __name__ == "__main__":
    # Example usage
    customers_df = generate_customers(customers=100, seed=42)
    print(f"DataFrame shape: {customers_df.shape}")
    print(f"DataFrame columns: {list(customers_df.columns)}")
    print("\nFirst 5 rows:")
    print(customers_df.head())

    inventory_df = generate_inventory_data(products=100, seed=42)
    print(f"DataFrame shape: {inventory_df.shape}")
    print(f"DataFrame columns: {list(inventory_df.columns)}")
    print("\nFirst 5 rows:")
    print(inventory_df.head())

    orders_df = generate_orders(orders=100, seed=42, inventory=inventory_df, customers=customers_df)
    print(f"DataFrame shape: {orders_df.shape}")
    print(f"DataFrame columns: {list(orders_df.columns)}")
    print("\nFirst 5 rows:")
    print(orders_df.head())
