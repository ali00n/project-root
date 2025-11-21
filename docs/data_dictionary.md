# 📘 Dicionário de Dados – Projeto Root

## 🏷 1. Camada RAW (sales_raw)
Dados brutos, extraídos de CSV, API ou banco, sem tratamento.

| Campo           | Tipo     | Descrição                                |
|----------------|----------|--------------------------------------------|
| transaction_id | int      | ID da transação                             |
| product_name   | string   | Nome do produto                            |
| quantity       | int      | Quantidade vendida                         |
| price          | float    | Preço unitário                             |
| date           | string   | Data no formato texto (YYYY-MM-DD)         |
| customer_id    | int      | ID do cliente                              |
| region         | string   | Região da venda                            |

---

## 🥉 2. Camada BRONZE (sales_bronze)
Dados estruturados, mesmo conteúdo da RAW, porém com tipos ajustados.

| Campo           | Tipo  | Descrição                          |
|----------------|-------|--------------------------------------|
| transaction_id | int   | ID da transação                     |
| product_name   | string| Nome do produto                     |
| quantity       | int   | Quantidade vendida                  |
| price          | float | Preço unitário                      |
| date           | date  | Data convertida                     |
| customer_id    | int   | ID do cliente                       |
| region         | string| Região da venda                     |

---

## 🥈 3. Camada SILVER (sales_silver)
Transformações aplicadas via `bronze_to_silver()`.

| Campo         | Tipo  | Descrição                               |
|---------------|--------|-------------------------------------------|
| transaction_id| int    | ID da transação                          |
| product_name  | string | Nome                                     |
| quantity      | int    | Quantidade                               |
| price         | float  | Preço unitário                           |
| total_sales   | float  | price * quantity                         |
| date          | date   | Data convertida                          |
| year          | int    | Ano                                      |
| month         | int    | Mês                                      |
| day           | int    | Dia                                      |
| customer_id   | int    | ID do cliente                            |
| region        | string | Região da venda                          |

---

# 🥇 4. Camada GOLD (agregações)

---

## 🟡 gold.monthly_sales

| Campo            | Tipo  | Descrição                     |
|------------------|--------|------------------------------|
| year             | int    | Ano                          |
| month            | int    | Mês                          |
| total_sales      | float  | Soma das vendas do mês       |
| total_transactions | int | Total de transações          |

---

## 🟡 gold.product_performance

| Campo         | Tipo  | Descrição                            |
|---------------|--------|----------------------------------------|
| product_name  | string | Nome do produto                       |
| total_quantity| int    | Quantidade total vendida              |
| total_sales   | float  | Receita total                         |

---

## 🟡 gold.regional_sales

| Campo             | Tipo  | Descrição                      |
|-------------------|--------|-------------------------------|
| region            | string | Região                        |
| total_sales       | float  | Receita por região            |
| total_transactions| int    | Número de vendas              |

