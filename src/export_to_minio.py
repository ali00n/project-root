from minio import Minio
import os

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

# ===============================
# CAMINHOS DOS ARQUIVOS
# ===============================
arquivos = {
    "silver/sample_data.csv": "src/datasets/sample_data.csv",
    "gold/sample_data.csv": "src/docs/sample_data.csv"
}

# ===============================
# UPLOAD
# ===============================
for destino, origem in arquivos.items():
    if not os.path.exists(origem):
        print(f"Arquivo não encontrado: {origem}")
        continue

    minio_client.fput_object(
        bucket_name,
        destino,
        origem
    )

    print(f"✔ {origem} enviado para MinIO → {destino}")

print("\nUPLOAD FINALIZADO COM SUCESSO")
