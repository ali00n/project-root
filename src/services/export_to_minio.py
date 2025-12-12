from minio import Minio
import os


class MinioUploader:
    def __init__(self):
        # ===============================
        # CONEXÃO MINIO
        # ===============================
        self.minio_client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

        self.bucket_name = "fipe"

        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)

        # ===============================
        # CAMINHOS DOS ARQUIVOS
        # ===============================
        self.arquivos = {
            "silver/sample_data.csv": "src/datasets/sample_data.csv",
            "gold/sample_data.csv": "src/docs/sample_data.csv"
        }

    def upload(self):
        # ===============================
        # UPLOAD
        # ===============================
        for destino, origem in self.arquivos.items():
            if not os.path.exists(origem):
                print(f"Arquivo não encontrado: {origem}")
                continue

            self.minio_client.fput_object(
                self.bucket_name,
                destino,
                origem
            )

            print(f"{origem} enviado para MinIO → {destino}")

        print("UPLOAD FINALIZADO COM SUCESSO")
