# pip install great-expectations

import great_expectations as ge
import pandas as pd
from pathlib import Path
from datetime import datetime

SILVER_PATH = Path("etl/silver/dados_tratados.parquet")

def run_quality_checks():
    df = pd.read_parquet(SILVER_PATH)
    ge_df = ge.from_pandas(df)

    # -------------------------
    # Schema
    # -------------------------
    ge_df.expect_column_to_exist("id")
    ge_df.expect_column_to_exist("email")
    ge_df.expect_column_to_exist("valor_compra")

    # -------------------------
    # Regras críticas
    # -------------------------
    ge_df.expect_column_values_to_not_be_null("id")
    ge_df.expect_column_values_to_be_unique("id")

    ge_df.expect_column_values_to_not_be_null("nome")
    ge_df.expect_column_values_to_not_be_null("cidade")

    ge_df.expect_column_values_to_be_between(
        "valor_compra",
        min_value=0,
        max_value=None
    )

    ge_df.expect_column_values_to_match_regex(
        "email",
        r".+@.+\..+"
    )

    ge_df.expect_column_values_to_be_between(
        "data_nascimento",
        min_value="1900-01-01",
        max_value=datetime.now().strftime("%Y-%m-%d")
    )

    # -------------------------
    # Tolerância a nulos
    # -------------------------
    ge_df.expect_column_values_to_not_be_null(
        "email",
        mostly=0.95  # aceita até 5% nulo
    )

    # -------------------------
    # Execução
    # -------------------------
    results = ge_df.validate()

    if not results["success"]:
        raise ValueError("Falha nos testes de qualidade de dados")

    print("✅ Testes de qualidade passaram com sucesso")

if __name__ == "__main__":
    run_quality_checks()
