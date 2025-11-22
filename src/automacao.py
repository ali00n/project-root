# -*- coding: utf-8 -*-
import psycopg2
from psycopg2 import sql
import matplotlib.pyplot as plt  # <-- adicionado aqui no topo

# Configurações de conexão
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "dd_project"
DB_USER = "postgres"
DB_PASS = "postgres"

def create_database_if_not_exists():
    """Cria o banco dd_project se não existir"""
    try:
        # Conecta no banco padrão 'postgres'
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname='postgres', user=DB_USER, password=DB_PASS)
        conn.autocommit = True  # Necessário para criar banco
        cur = conn.cursor()

        # Verifica se o banco existe
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        exists = cur.fetchone()
        if exists:
            print(f"O banco '{DB_NAME}' já existe.\n")
        else:
            cur.execute(sql.SQL(
                "CREATE DATABASE {} ENCODING 'UTF8' LC_COLLATE='Portuguese_Brazil.1252' LC_CTYPE='Portuguese_Brazil.1252';"
            ).format(sql.Identifier(DB_NAME)))
            print(f"Banco '{DB_NAME}' criado com sucesso!\n")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] Criando banco: {e}")
        exit(1)

def execute_queries(conn, queries):
    """Executa uma lista de queries usando a conexão passada"""
    with conn.cursor() as cur:
        for query in queries:
            try:
                cur.execute(query)
            except Exception as e:
                print(f"[ERRO] Query falhou:\n{query}\n{e}")
    conn.commit()

def fetch_query(conn, query):
    """Executa uma query de seleção e retorna resultados"""
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def main():
    print("AUTOMAÇÃO DE PROCESSO DE INSERTS, SELECTS E VALIDAÇÃO...\n")

    # 🔹 Novo passo: criar banco se não existir
    create_database_if_not_exists()

    # 🔹 Conexão com o banco
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conectado com sucesso ao PostgreSQL!\n")
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return

    # 🔹 O resto do seu código original continua exatamente igual
    # Criar schemas
    schemas = [
        "CREATE SCHEMA IF NOT EXISTS bronze;",
        "CREATE SCHEMA IF NOT EXISTS silver;",
        "CREATE SCHEMA IF NOT EXISTS gold;"
    ]
    execute_queries(conn, schemas)
    print("Schemas criados ou já existentes.\n")

    # Criar tabelas
    tables = [
        """CREATE TABLE IF NOT EXISTS bronze.sales_bronze(
            id SERIAL PRIMARY KEY,
            date DATE,
            product_name TEXT,
            customer_id INTEGER,
            amount NUMERIC
        );""",
        """CREATE TABLE IF NOT EXISTS silver.sales_silver(
            id SERIAL PRIMARY KEY,
            date DATE,
            product_name TEXT,
            region TEXT,
            year INTEGER,
            month INTEGER,
            amount NUMERIC
        );""",
        """CREATE TABLE IF NOT EXISTS gold.monthly_sales(
            id SERIAL PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            total_sales NUMERIC
        );""",
        """CREATE TABLE IF NOT EXISTS gold.product_performance(
            id SERIAL PRIMARY KEY,
            product_name TEXT,
            total_sales NUMERIC
        );""",
        """CREATE TABLE IF NOT EXISTS gold.regional_sales(
            id SERIAL PRIMARY KEY,
            region TEXT,
            total_sales NUMERIC
        );"""
    ]
    execute_queries(conn, tables)
    print("Tabelas criadas ou já existentes.\n")

    # Inserir dados de teste
    inserts = [
        # Bronze
        """INSERT INTO bronze.sales_bronze (date, product_name, customer_id, amount) VALUES
        ('2025-11-01', 'Produto A', 1, 100.50),
        ('2025-11-02', 'Produto B', 2, 200.00),
        ('2025-11-03', 'Produto C', 3, 150.75),
        ('2025-11-04', 'Produto A', 4, 300.00),
        ('2025-11-05', 'Produto B', 5, 50.25);""",

        # Silver
        """INSERT INTO silver.sales_silver (date, product_name, region, year, month, amount) VALUES
        ('2025-11-01', 'Produto A', 'Norte', 2025, 11, 100.50),
        ('2025-11-02', 'Produto B', 'Sul', 2025, 11, 200.00),
        ('2025-11-03', 'Produto C', 'Leste', 2025, 11, 150.75),
        ('2025-11-04', 'Produto A', 'Oeste', 2025, 11, 300.00),
        ('2025-11-05', 'Produto B', 'Norte', 2025, 11, 50.25);""",

        # Gold
        """INSERT INTO gold.monthly_sales (year, month, total_sales) VALUES (2025, 11, 801.50);""",

        """INSERT INTO gold.product_performance (product_name, total_sales) VALUES
        ('Produto A', 400.50),
        ('Produto B', 250.25),
        ('Produto C', 150.75);""",

        """INSERT INTO gold.regional_sales (region, total_sales) VALUES
        ('Norte', 150.75),
        ('Sul', 200.00),
        ('Leste', 150.75),
        ('Oeste', 300.00);"""
    ]
    execute_queries(conn, inserts)
    print("Dados de teste inseridos.\n")

    # Consultas de validação
    validation_queries = {
        "Bronze - todas as vendas": "SELECT * FROM bronze.sales_bronze;",
        "Silver - total por produto": "SELECT product_name, SUM(amount) AS total_vendas FROM silver.sales_silver GROUP BY product_name ORDER BY total_vendas DESC;",
        "Silver - total por região": "SELECT region, SUM(amount) AS total_vendas FROM silver.sales_silver GROUP BY region ORDER BY total_vendas DESC;",
        "Gold - total mensal": "SELECT year, month, total_sales FROM gold.monthly_sales;",
        "Gold - performance por produto": "SELECT product_name, total_sales FROM gold.product_performance ORDER BY total_sales DESC;",
        "Gold - vendas por região": "SELECT region, total_sales FROM gold.regional_sales ORDER BY total_sales DESC;"
    }

    print("--- Resultados de validação ---\n")
    for name, query in validation_queries.items():
        print(f"{name}:")
        try:
            results = fetch_query(conn, query)
            for row in results:
                print(", ".join(str(x) for x in row))

            # 🔹 Implementação Matplotlib apenas para visualização
            if name == "Silver - total por produto":
                produtos = [r[0] for r in results]
                totais = [r[1] for r in results]
                plt.figure(figsize=(8, 5))
                plt.bar(produtos, totais, color='skyblue', edgecolor='black')
                plt.title("Total de Vendas por Produto (Silver)")
                plt.xlabel("Produto")
                plt.ylabel("Total Vendas")
                for i, v in enumerate(totais):
                    plt.text(i, v + 5, str(round(v, 2)), ha='center')
                plt.show()

            elif name == "Gold - vendas por região":
                regioes = [r[0] for r in results]
                vendas = [r[1] for r in results]
                plt.figure(figsize=(8, 5))
                plt.pie(vendas, labels=regioes, autopct='%1.1f%%', startangle=140,
                        colors=['#66b3ff', '#99ff99', '#ff9999', '#ffcc99'])
                plt.title("Vendas por Região (Gold)")
                plt.show()
                plt.show(block=True)


        except Exception as e:
            print(f"[ERRO] Consulta falhou: {e}")

    conn.close()
    print("Automação finalizada com sucesso!")

if __name__ == "__main__":
    main()
