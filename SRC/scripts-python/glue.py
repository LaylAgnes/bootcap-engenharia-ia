import boto3

glue = boto3.client("glue", region_name="us-east-1")

DATABASE_NAME = "silver"
TABLE_NAME = "clientes"
S3_LOCATION = "s3://bootcamp-ia-data-lake/silver/clientes/"

def create_or_update_table():
    table_input = {
        "Name": TABLE_NAME,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "nome", "Type": "string"},
                {"Name": "email", "Type": "string"},
                {"Name": "data_nascimento", "Type": "date"},
                {"Name": "cidade", "Type": "string"},
                {"Name": "valor_compra", "Type": "double"},
            ],
            "Location": S3_LOCATION,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
        },
    }

    try:
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input
        )
        print("✅ Tabela criada no Glue Catalog")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input
        )
        print("🔄 Tabela atualizada no Glue Catalog")

if __name__ == "__main__":
    create_or_update_table()
