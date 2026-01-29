import pandas as pd
import re
from pathlib import Path

# -------------------------
# Configurações
# -------------------------
RAW_PATH = Path("etl/raw/dados_brutos.csv")
SILVER_PATH = Path("etl/silver/dados_tratados.parquet")

EXPECTED_COLUMNS = [
    "id",
    "nome",
    "email",
    "data_nascimento",
    "cidade",
    "valor_compra"
]

# -------------------------
# Funções auxiliares
# -------------------------
def read_file(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    elif path.suffix in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    else:
        raise ValueError("Formato não suportado")


def normalize_raw_column(value: str) -> list:
    """
    Normaliza delimitadores e quebra a linha em campos
    """
    if pd.isna(value):
        return [None] * len(EXPECTED_COLUMNS)

    # Padroniza delimitadores para ;
    value = re.sub(r"[|,-]", ";", str(value))

    parts = [p.strip() for p in value.split(";")]

    # Garante tamanho correto
    if len(parts) < len(EXPECTED_COLUMNS):
        parts.extend([None] * (len(EXPECTED_COLUMNS) - len(parts)))

    return parts[:len(EXPECTED_COLUMNS)]


# -------------------------
# ETL
# -------------------------
def run_etl():
    df_raw = read_file(RAW_PATH)

    # Assume que tudo está na primeira coluna
    raw_column = df_raw.columns[0]

    parsed_data = df_raw[raw_column].apply(normalize_raw_column)

    df_structured = pd.DataFrame(
        parsed_data.tolist(),
        columns=EXPECTED_COLUMNS
    )

    # -------------------------
    # Limpeza de dados
    # -------------------------
    df_structured["id"] = pd.to_numeric(df_structured["id"], errors="coerce")

    df_structured["valor_compra"] = (
        df_structured["valor_compra"]
        .astype(str)
        .str.replace(",", ".", regex=False)
    )
    df_structured["valor_compra"] = pd.to_numeric(
        df_structured["valor_compra"], errors="coerce"
    )

    df_structured["data_nascimento"] = pd.to_datetime(
        df_structured["data_nascimento"], errors="coerce"
    )

    df_structured["email"] = df_structured["email"].str.lower().str.strip()
    df_structured["nome"] = df_structured["nome"].str.title().str.strip()
    df_structured["cidade"] = df_structured["cidade"].str.title().str.strip()

    # Remove linhas totalmente inválidas
    df_structured = df_structured.dropna(subset=["id"])

    # -------------------------
    # Output Silver
    # -------------------------
    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_structured.to_parquet(SILVER_PATH, index=False)

    print("ETL finalizado com sucesso")
    print(df_structured.info())


if __name__ == "__main__":
    run_etl()