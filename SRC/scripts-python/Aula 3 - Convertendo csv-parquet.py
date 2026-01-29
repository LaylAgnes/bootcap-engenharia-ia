import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CSV_PATH = r"C:\\Users\\de0189462\\Documents\\Bootcamp Engenharia IA\\vendas_1M.csv"
PARQUET_PATH = r"C:\\Users\\de0189462\\Documents\\Bootcamp Engenharia IA\\SRC\\vendas_1M.parquet"

CHUNK_SIZE = 200_000

parquet_writer = None

print("Convertendo CSV → Parquet...")

for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)):
    print(f"Processando chunk {i+1}")

    table = pa.Table.from_pandas(chunk, preserve_index=False)

    if parquet_writer is None:
        parquet_writer = pq.ParquetWriter(PARQUET_PATH, table.schema, compression="snappy")

    parquet_writer.write_table(table)

if parquet_writer:
    parquet_writer.close()

print("Parquet gerado em:")
print(PARQUET_PATH)
