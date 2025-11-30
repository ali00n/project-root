# -*- coding: utf-8 -*-
import psycopg2

# Configurações de conexão
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "dd_project"
DB_USER = "postgres"
DB_PASS = "postgres"

def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print(f"Conexão com o banco '{DB_NAME}' realizada com sucesso!")
        conn.close()
    except Exception as e:
        print(f"Erro ao conectar ao banco '{DB_NAME}': {e}")

if __name__ == "__main__":
    main()
