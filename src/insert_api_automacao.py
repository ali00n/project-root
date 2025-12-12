# -*- coding: utf-8 -*-
import time
import os
import csv
import matplotlib.pyplot as plt
from services.fipe_api_client import FipeApiClient, parse_valor_fipe
from src.tests.test_db_connection import DBConnection
from services.delete_table import DatabaseCleaner
from services.export_to_minio import MinioUploader



class ApiFipe:
    # ================================================================
    #   INSERÇÃO — BRONZE
    # ================================================================
    def insert_bronze(self, conn, registros):
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.fipe_raw (
                id SERIAL PRIMARY KEY,
                marca VARCHAR(50) NOT NULL,
                modelo VARCHAR(100) NOT NULL,
                ano_modelo VARCHAR(10) NOT NULL,
                codigo_marca INTEGER NOT NULL,
                codigo_modelo INTEGER NOT NULL,
                codigo_ano VARCHAR(20) NOT NULL,
                valor VARCHAR(30),
                valor_numeric NUMERIC(12,2)
            );
        """)

        cur.execute("DELETE FROM bronze.fipe_raw;")

        for r in registros:
            cur.execute("""
                INSERT INTO bronze.fipe_raw
                (marca, modelo, ano_modelo, codigo_marca, codigo_modelo, codigo_ano, valor, valor_numeric)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                r["marca"],
                r["modelo"],
                r["ano"],
                r["cod_marca"],
                r["cod_modelo"],
                r["cod_ano"],
                r["valor_str"],
                r["valor_num"]
            ))

        conn.commit()
        print(f"BRONZE OK! Inseridos {len(registros)} registros")

    # ================================================================
    #   INSERÇÃO — SILVER (18k–30k)
    # ================================================================
    def insert_silver(self, conn):
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver.fipe_limited (
                id SERIAL PRIMARY KEY,
                marca VARCHAR(50) NOT NULL,
                modelo VARCHAR(100) NOT NULL,
                ano_modelo VARCHAR(10) NOT NULL,
                valor_numeric NUMERIC(12,2)
            );
        """)

        cur.execute("DELETE FROM silver.fipe_limited;")

        cur.execute("""
            INSERT INTO silver.fipe_limited (marca, modelo, ano_modelo, valor_numeric)
            SELECT marca, modelo, ano_modelo, valor_numeric
            FROM bronze.fipe_raw
            WHERE valor_numeric BETWEEN 18000 AND 30000
            ORDER BY valor_numeric DESC;
        """)

        conn.commit()
        print("SILVER OK! (18k–30k filtrado)")

    # ================================================================
    #   INSERÇÃO — GOLD
    # ================================================================
    def insert_gold(self, conn):
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS gold.fipe_summary (
                id SERIAL PRIMARY KEY,
                marca VARCHAR(50) NOT NULL,
                modelo VARCHAR(100) NOT NULL,
                media_valor NUMERIC(12,2),
                qtd_registros INTEGER
            );
        """)

        cur.execute("DELETE FROM gold.fipe_summary;")

        cur.execute("""
            INSERT INTO gold.fipe_summary (marca, modelo, media_valor, qtd_registros)
            SELECT 
                marca,
                modelo,
                AVG(valor_numeric),
                COUNT(*)
            FROM silver.fipe_limited
            GROUP BY marca, modelo
            ORDER BY AVG(valor_numeric) DESC;
        """)

        conn.commit()
        print("GOLD OK! (médias por modelo)")

    # ================================================================
    #   GRÁFICO — TOP 10 MAIORES VALORES (SILVER)
    # ================================================================
    def gerar_grafico(self, conn):
        cur = conn.cursor()

        cur.execute("""
            SELECT modelo, valor_numeric
            FROM silver.fipe_limited
            ORDER BY valor_numeric DESC
            LIMIT 10;
        """)

        rows = cur.fetchall()

        if not rows:
            print("Nenhum dado para gráficos!")
            return

        modelos = [r[0] for r in rows]
        valores = [r[1] for r in rows]

        os.makedirs("datasets", exist_ok=True)
        csv_path = "datasets/sample_data.csv"

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Modelo", "Valor"])
            writer.writerows(rows)

        print(f"Dados salvos em {csv_path}")

        plt.figure(figsize=(10, 5))
        plt.bar(modelos, valores, edgecolor="black")
        plt.xticks(rotation=45, ha='right')
        plt.title("TOP 10 Motos FIPE — Faixa 18k a 30k")
        plt.xlabel("Modelo")
        plt.ylabel("Valor (R$)")
        plt.tight_layout()
        plt.show()

    # ================================================================
    #   LIMPEZA DO BANCO ANTES DA INSERÇÃO
    # ================================================================
    def _limpar_banco_antes_insercao(self):
        """
        Verifica e limpa o banco de dados antes de inserir novos dados
        """
        print("VERIFICAÇÃO DO BANCO DE DADOS...")

        # Criar conexão para o cleaner
        banco_temp = DBConnection(
            host="localhost",
            port=5432,
            dbname="fipe_banco",
            user="postgres",
            password="postgres"
        )

        # Usar o DatabaseCleaner para verificar e limpar
        cleaner = DatabaseCleaner(banco_temp)
        dados_foram_limpos = cleaner.check_and_clean_database()

        if dados_foram_limpos:
            print("Banco limpo com sucesso. Pronto para novos dados.")
        else:
            print("Banco já estava vazio. Pronto para novos dados.")

        return True

    # ================================================================
    #   MAIN - PARTE PRINCIPAL DO PROJETO
    # ================================================================
    def main(self):
        print("\n" + "=" * 60)
        print("COLETA FIPE (Honda + Yamaha — 10 modelos cada)")

        # PASSO 1: Limpar banco antes de começar
        self._limpar_banco_antes_insercao()

        # PASSO 2: Conectar ao banco para inserção
        banco = DBConnection(
            host="localhost",
            port=5432,
            dbname="fipe_banco",
            user="postgres",
            password="postgres"
        )

        conn = banco.connect()
        if not conn:
            print("ERRO: Não foi possível conectar ao banco de dados!")
            return

        # PASSO 3: Coletar dados da API
        api = FipeApiClient()
        marcas_desejadas = {"HONDA", "YAMAHA"}
        registros_final = []

        print("\n" + "=" * 60)
        print("COLETANDO DADOS DA API FIPE")
        print("=" * 60 + "\n")

        print("Buscando marcas...")
        marcas = api.get_marcas()

        for m in marcas:
            nome = m["nome"].upper()
            if nome not in marcas_desejadas:
                continue

            print(f"\n{'=' * 40}")
            print(f"MARCA: {nome}")
            print(f"{'=' * 40}")

            modelos = api.get_modelos(m["codigo"])
            modelos_coletados = 0

            for mod in modelos:
                if modelos_coletados >= 10:
                    print(f"Limite de {modelos_coletados} modelos atingido para {nome}.")
                    break

                print(f"→ Modelo: {mod['nome']}")
                anos = api.get_anos(m["codigo"], mod["codigo"])

                encontrou_preco = False

                for ano in anos:
                    preco = api.get_preco(m["codigo"], mod["codigo"], ano["codigo"])
                    time.sleep(0.8)

                    if not preco:
                        continue

                    valor_num = parse_valor_fipe(preco.get("Valor"))
                    valor_str = preco.get("Valor")

                    registros_final.append({
                        "marca": nome,
                        "modelo": mod["nome"],
                        "ano": ano["codigo"],
                        "cod_marca": m["codigo"],
                        "cod_modelo": mod["codigo"],
                        "cod_ano": ano["codigo"],
                        "valor_str": valor_str,
                        "valor_num": valor_num
                    })

                    print(f"✓ {nome} - {mod['nome']} - {ano['codigo']} → {valor_str}")
                    encontrou_preco = True

                if encontrou_preco:
                    modelos_coletados += 1

            print(f"✓ Total coletado da marca {nome}: {modelos_coletados} modelos")

        print(f"\n{'=' * 60}")
        print(f"TOTAL GERAL COLETADO: {len(registros_final)} registros.")
        print(f"{'=' * 60}\n")

        # PASSO 4: Processar dados (bronze, silver, gold)
        print("\n" + "=" * 60)
        print("PROCESSAMENTO DOS DADOS (BRONZE → SILVER → GOLD)")
        print("=" * 60 + "\n")

        self.insert_bronze(conn, registros_final)
        self.insert_silver(conn)
        self.insert_gold(conn)

        # PASSO 5: Gerar gráficos
        print("\n" + "=" * 60)
        print("GERAÇÃO DE GRÁFICOS E RELATÓRIOS")
        print("=" * 60 + "\n")

        self.gerar_grafico(conn)

        # PASSO 6: Enviar arquivos para o MinIO
        print("\n" + "=" * 60)
        print("EXPORTAÇÃO DOS DADOS PARA O MINIO")
        print("=" * 60 + "\n")

        uploader = MinioUploader()
        uploader.upload()

        # Fechar conexão
        conn.close()

        print("\n" + "=" * 60)
        print("PROCESSO CONCLUÍDO COM SUCESSO!")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    # EXECUTE O PIPELINE DA FIPE
    ApiFipe().main()