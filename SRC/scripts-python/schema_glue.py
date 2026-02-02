import yaml

TYPE_MAPPING = {
    "int": "int",
    "float": "double",
    "string": "string",
    "boolean": "boolean"
}

def load_contract(path):
    with open(path) as f:
        return yaml.safe_load(f)

def contract_to_glue_columns(contract):
    columns = []
    for col in contract["columns"]:
        columns.append({
            "Name": col["name"],
            "Type": TYPE_MAPPING[col["type"]]
        })
    return columns
