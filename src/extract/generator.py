from faker import Faker
import random
from typing import List, Dict

fake = Faker()

def generate_products(num_records: int) -> List[Dict]:
    """
    Generates a list of fake product dictionaries.
    """
    products = []
    for _ in range(num_records):
        price = round(random.uniform(10, 1000), 2)
        # Introduce "Garbage Data" (Negative Price) randomly (1% chance)
        if random.random() < 0.01:
            price = -price
            
        product = {
            "product_id": fake.uuid4(),
            "name": fake.catch_phrase()[:100], # Sometimes valid
            "price": price,
            "category": fake.word(),
            "created_at": fake.iso8601()
        }
        
        # Garlic Data: Missing Name (1% chance)
        if random.random() < 0.01:
            product["name"] = None
            
        products.append(product)
    return products
