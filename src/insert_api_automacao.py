# -*- coding: utf-8 -*-
import psycopg2
import time
import os
import csv
import matplotlib.pyplot as plt
from services.fipe_api_client import FipeApiClient, parse_valor_fipe

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "fipe_banco"
DB_USER = "postgres"
DB_PASS = "postgres"


# ================================================================
#   CONEXÃO COM O BANCO
# ================================================================
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conectado ao banco PostgreSQL com sucesso!\n")
        return conn
    except Exception as e:
        print(f"[ERRO] Conexão falhou: {e}")
        exit(1)


# ================================================================
#   INSERÇÃO — BRONZE
# ================================================================
def insert_bronze(conn, registros):
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
    print(f"BRONZE OK! Inseridos {len(registros)} registros.\n")


# ================================================================
#   INSERÇÃO — SILVER (18k–30k)
# ================================================================
def insert_silver(conn):
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
    print("SILVER OK! (18k–30k filtrado)\n")


# ================================================================
#   INSERÇÃO — GOLD
# ================================================================
def insert_gold(conn):
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
    print("GOLD OK! (médias por modelo)\n")


# ================================================================
#   GRÁFICO — TOP 10 MAIORES VALORES (SILVER)
# ================================================================
def gerar_grafico(conn):
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
#   MAIN
# ================================================================
def main():
    print("\n=== COLETA FIPE (Honda + Yamaha — 10 modelos cada) ===\n")

    conn = connect_db()
    api = FipeApiClient()

    marcas_desejadas = {"HONDA", "YAMAHA"}
    registros_final = []

    print("Buscando marcas...")
    marcas = api.get_marcas()

    for m in marcas:
        nome = m["nome"].upper()
        if nome not in marcas_desejadas:
            continue

        print(f"\n=== MARCA: {nome} ===")

        modelos = api.get_modelos(m["codigo"])
        modelos_coletados = 0  # controla quantos modelos já foram coletados

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

        print(f"✓ Total coletado da marca {nome}: {modelos_coletados} modelos\n")

    print(f"\nTOTAL GERAL COLETADO: {len(registros_final)} registros.\n")

    insert_bronze(conn, registros_final)
    print("REGISTROS FINAL:", len(registros_final))

    insert_silver(conn)
    insert_gold(conn)
    gerar_grafico(conn)

    print("\nProcesso concluído com sucesso!\n")


if __name__ == "__main__":
    main()
