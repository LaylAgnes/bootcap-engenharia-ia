import pandas as pd
import yaml
from pathlib import Path

DATA_PATH = Path("etl/silver/dados_tratados.parquet")
SCHEMA_PATH = Path("data_contracts/silver/schema_v1.yaml")

TYPE_MAPPING = {
    "int": "int64",
    "float": "float64",
    "string": "object",
    "date": "datetime64[ns]"
}

def load_schema():
    with open(SCHEMA_PATH) as f:
        return yaml.safe_load(f)

def validate_schema(df, schema):
    errors = []

    for col in schema["columns"]:
        name = col["name"]

        # coluna existe
        if name not in df.columns:
            errors.append(f"Coluna ausente: {name}")
            continue

        # tipo
        expected_type = TYPE_MAPPING[col["type"]]
        if str(df[name].dtype) != expected_type:
            errors.append(
                f"Tipo inválido em {name}: "
                f"{df[name].dtype} != {expected_type}"
            )

        # nulos
        if not col.get("nullable", True) and df[name].isna().any():
            errors.append(f"Nulos não permitidos em {name}")

        # unicidade
        if col.get("unique") and not df[name].is_unique:
            errors.append(f"Valores duplicados em {name}")

        # mínimo
        if "min" in col and (df[name] < col["min"]).any():
            errors.append(f"Valor abaixo do mínimo em {name}")

    if errors:
        raise ValueError("❌ Contrato de dados violado:\n" + "\n".join(errors))

    print("✅ Contrato de dados validado com sucesso")

def main():
    df = pd.read_parquet(DATA_PATH)
    schema = load_schema()
    validate_schema(df, schema)

if __name__ == "__main__":
    main()
