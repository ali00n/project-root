# -*- coding: utf-8 -*-
import psycopg2
import matplotlib.pyplot as plt
import time
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
#   INSERÇÃO — GOLD (média por modelo)
# ================================================================
def insert_gold(conn):
    cur = conn.cursor()

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
#   GRÁFICO – TOP 10 MAIORES VALORES (SILVER)
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

    plt.figure(figsize=(10, 5))
    plt.bar(modelos, valores, edgecolor='black')
    plt.xticks(rotation=45, ha='right')
    plt.title("Top 10 Motos FIPE — Faixa 18k a 30k")
    plt.xlabel("Modelo")
    plt.ylabel("Valor (R$)")
    plt.tight_layout()
    plt.show()


# ================================================================
#   MAIN
# ================================================================
def main():
    print("\n=== COLETA FIPE (Honda + Yamaha) ===\n")
    conn = connect_db()
    api = FipeApiClient()

    marcas_desejadas = {"HONDA", "YAMAHA"}
    registros_final = []

    print("📥 Buscando marcas...")
    marcas = api.get_marcas()

    for m in marcas:
        nome = m["nome"].upper()
        if nome not in marcas_desejadas:
            continue

        print(f"\n=== MARCA: {nome} ===")
        modelos = api.get_modelos(m["codigo"])

        for mod in modelos:
            print(f"→ Modelo: {mod['nome']}")
            anos = api.get_anos(m["codigo"], mod["codigo"])

            for ano in anos:
                preco = api.get_preco(m["codigo"], mod["codigo"], ano["codigo"])
                time.sleep(0.2)  # <-- DELAY para evitar bloqueio

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

    print(f"\nTotal coletado: {len(registros_final)} registros.\n")

    # Inserções
    insert_bronze(conn, registros_final)
    insert_silver(conn)
    insert_gold(conn)

    # Gráficos
    gerar_grafico(conn)

    print("\nProcesso concluído com sucesso!\n")


if __name__ == "__main__":
    main()
