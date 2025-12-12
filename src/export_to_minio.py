import psycopg2
import csv
import os
from minio import Minio

# ===============================
# CONEXÃO POSTGRES
# ===============================
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="fipe_banco",
    user="postgres",
    password="postgres"
)

# ===============================
# CONEXÃO MINIO
# ===============================
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "fipe"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

os.makedirs("datasets", exist_ok=True)

# ===============================
# FUNÇÃO EXPORTAR TABELA
# ===============================
def exportar_tabela(schema, tabela, arquivo):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {schema}.{tabela}")
    rows = cur.fetchall()

    colnames = [desc[0] for desc in cur.description]

    with open(arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)

    minio_client.fput_object(
        bucket_name,
        f"{schema}/{tabela}.csv",
        arquivo
    )

    print(f"✔ {schema}.{tabela} enviado para o MinIO")

# ===============================
# EXPORTA SILVER E GOLD
# ===============================
exportar_tabela("silver", "fipe_limited", "datasets/fipe_limited.csv")
exportar_tabela("gold", "fipe_summary", "datasets/fipe_summary.csv")

conn.close()
