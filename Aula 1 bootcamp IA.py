from faker import Faker
import pandas as pd
import random

fake = Faker("pt_BR")

NUM_ROWS = 1_000_000  # 1 milhão de registros

products = [
    ("Notebook", "Eletrônicos", 3500),
    ("Mouse", "Eletrônicos", 150),
    ("Teclado", "Eletrônicos", 200),
    ("Monitor", "Eletrônicos", 1200),
    ("Cadeira Gamer", "Móveis", 1800),
    ("Mesa Escritório", "Móveis", 2200),
    ("Headset", "Eletrônicos", 400),
]

data = []

for i in range(NUM_ROWS):
    product, category, base_price = random.choice(products)
    quantity = random.randint(1, 5)
    price = base_price * random.uniform(0.9, 1.1)
    total_value = price * quantity

    data.append({
        "order_id": i + 1,
        "customer_id": random.randint(1, 300_000),
        "customer_name": fake.name(),
        "email": fake.email(),
        "product": product,
        "category": category,
        "price": round(price, 2),
        "quantity": quantity,
        "total_value": round(total_value, 2),
        "order_date": fake.date_between(start_date="-2y", end_date="today"),
        "country": fake.country()
    })

df = pd.DataFrame(data)

df.to_csv("vendas_1M.csv", index=False)

print("Dataset gerado com sucesso: vendas_1M.csv")
