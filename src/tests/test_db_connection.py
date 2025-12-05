# -*- coding: utf-8 -*-
import psycopg2


class DBConnection:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            print(f"Conectado ao banco '{self.dbname}' com sucesso!")
            return self.connection
        except Exception as e:
            print(f"[ERRO] Falha ao conectar: {e}")
            return None

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Conexão encerrada.")

    def get_cursor(self):
        if self.connection:
            return self.connection.cursor()
        return None


if __name__ == "__main__":
    # Teste da conexão
    db = DBConnection("localhost", 5432, "api_fipe", "postgres", "postgres")
    conn = db.connect()

    if conn:
        # Teste simples
        cur = conn.cursor()
        cur.execute("SELECT version();")

        db.disconnect()