import csv
import random
from faker import Faker
from tqdm import tqdm

fake = Faker("pt_BR")

TOTAL = 1_000_000
OUTPUT = "vendas_1M."

produtos = [
    "Notebook", "Mouse", "Teclado", "Monitor", "Celular", "Tablet",
    "Cadeira Gamer", "Mesa", "Headset", "Webcam", "Impressora",
    "Scanner", "HD Externo", "SSD", "Pen Drive", "Roteador"
]

status_pedido = ["Pago", "Cancelado", "Pendente", "Estornado", "Enviado", "Entregue"]
formas_pagamento = ["Cartão", "Pix", "Boleto", "Transferência", "Dinheiro"]
canais = ["Site", "App", "Loja Física", "WhatsApp", "Marketplace"]

with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    header = [
        "id_venda","data_venda","data_entrega","cliente_nome","cliente_email",
        "cpf","cidade","estado","produto","categoria",
        "quantidade","preco_unitario","desconto","valor_total",
        "status","forma_pagamento","canal_venda",
        "vendedor","comissao","custo_produto","lucro","avaliacao"
    ]
    writer.writerow(header)

    for i in tqdm(range(1, TOTAL + 1)):
        produto = random.choice(produtos)
        categoria = random.choice(["Eletrônicos", "Informática", "Móveis", "Acessórios"])

        quantidade = random.randint(1, 10)
        preco = round(random.uniform(10, 5000), 2)

        # 5% dos preços inválidos
        if random.random() < 0.05:
            preco = random.choice([0, -50, 99999])

        desconto = round(random.uniform(0, preco * 1.2), 2)

        valor_total = (quantidade * preco) - desconto

        # 3% de valores quebrados
        if random.random() < 0.03:
            valor_total = random.choice([0, -100, 999999])

        data_venda = fake.date_between(start_date="-2y", end_date="today")
        data_entrega = fake.date_between(start_date="-2y", end_date="+30d")

        # 5% entregas antes da venda (erro realista)
        if random.random() < 0.05:
            data_entrega = fake.date_between(start_date="-3y", end_date=data_venda)

        cpf = fake.cpf()
        if random.random() < 0.08:
            cpf = str(random.randint(100000000, 999999999))  # inválido

        status = random.choice(status_pedido)

        custo = round(preco * random.uniform(0.4, 0.9), 2)
        comissao = round(valor_total * random.uniform(0.01, 0.08), 2)
        lucro = round(valor_total - custo - comissao, 2)

        # 7% de campos vazios
        def maybe_null(val):
            return val if random.random() > 0.07 else ""

        writer.writerow([
            i,
            data_venda,
            data_entrega,
            maybe_null(fake.name()),
            maybe_null(fake.email()),
            cpf,
            fake.city(),
            fake.state_abbr(),
            produto,
            categoria,
            quantidade,
            preco,
            desconto,
            valor_total,
            status,
            random.choice(formas_pagamento),
            random.choice(canais),
            fake.first_name(),
            comissao,
            custo,
            lucro,
            random.randint(1,5) if random.random() > 0.1 else ""
        ])

print("Arquivo gerado:", OUTPUT)
