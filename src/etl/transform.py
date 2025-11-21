# src/etl/transform.py
import json
from typing import Dict
import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpeza básica de dados:
    - remove duplicatas exatas
    - preenche nomes nulos com 'unknown'
    - preenche age nulo com mediana
    - converte coluna 'date' ou 'order_date' para datetime
    - garante colunas esperadas básicas
    Retorna um DataFrame limpo (cópia).
    """
    if df is None or df.empty:
        return df.copy()

    df = df.copy()

    # Normalizar nomes de coluna comuns
    if "order_date" in df.columns and "date" not in df.columns:
        df.rename(columns={"order_date": "date"}, inplace=True)

    # Remove duplicatas exatas
    df = df.drop_duplicates()

    # Preenche valores nulos gerenciáveis
    if "name" in df.columns:
        df["name"] = df["name"].fillna("Unknown")

    if "age" in df.columns:
        try:
            med = pd.to_numeric(df["age"], errors="coerce").median()
            df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(med).astype(int)
        except Exception:
            # se algo falhar, apenas preencher com 0
            df["age"] = df["age"].fillna(0)

    # Converter data para datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformações de negócio para dados de vendas:
    - cria coluna total_sales = price * quantity (se existirem)
    - extrai year/month/day da coluna date (se existir)
    - retorna um novo DataFrame com tipos coerentes
    """
    if df is None or df.empty:
        # retornar um df vazio com as colunas padrão
        return pd.DataFrame(columns=[
            "product_name", "price", "quantity", "total_sales", "date", "year", "month", "day"
        ])

    df = df.copy()

    # Garantir colunas numéricas
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    else:
        df["price"] = 0.0

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    else:
        df["quantity"] = 0

    # Calcular total_sales
    df["total_sales"] = df["price"] * df["quantity"]

    # Converter/extrair data
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["day"] = df["date"].dt.day
    else:
        df["date"] = pd.NaT
        df["year"] = pd.NA
        df["month"] = pd.NA
        df["day"] = pd.NA

    return df


def raw_to_bronze(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte uma tabela raw (por exemplo com coluna 'payload' contendo JSON)
    para um DataFrame estruturado (bronze).
    - Se df contém coluna 'payload' (JSON strings ou dicts), extrai campos comuns:
      order_id, customer_id, order_date, total_amount (se existirem).
    - Se o DataFrame já parece estruturado (tem order_id ou customer_id), retorna cópia.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Se existe coluna payload, parsear JSON
    if "payload" in df.columns:
        rows = []
        for idx, payload in df["payload"].iteritems():
            try:
                if isinstance(payload, (str, bytes)):
                    parsed = json.loads(payload)
                elif isinstance(payload, dict):
                    parsed = payload
                else:
                    parsed = {}
            except Exception:
                parsed = {}

            # Tentar mapear campos comuns
            row = {
                "order_id": parsed.get("order_id") or parsed.get("id") or None,
                "customer_id": parsed.get("customer_id") or parsed.get("customer") or None,
                "order_date": parsed.get("order_date") or parsed.get("date") or None,
                "total_amount": parsed.get("total_amount") or parsed.get("amount") or None,
            }
            rows.append(row)

        bronze = pd.DataFrame(rows)
        # Ajustes de tipos
        if "order_date" in bronze.columns:
            bronze["order_date"] = pd.to_datetime(bronze["order_date"], errors="coerce")
        if "total_amount" in bronze.columns:
            bronze["total_amount"] = pd.to_numeric(bronze["total_amount"], errors="coerce").fillna(0.0)
        return bronze

    # Se já contém colunas estruturadas, retornar apenas cópia com nomes padronizados
    # Aceitar tanto order_date quanto date
    if "order_date" in df.columns or "date" in df.columns:
        out = df.copy()
        if "date" in out.columns and "order_date" not in out.columns:
            out = out.rename(columns={"date": "order_date"})
        # garantir tipos
        out["order_date"] = pd.to_datetime(out["order_date"], errors="coerce")
        if "total_amount" in out.columns:
            out["total_amount"] = pd.to_numeric(out["total_amount"], errors="coerce").fillna(0.0)
        return out

    # Caso não reconheça, retorna o df original (cópia)
    return df.copy()


def bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte a camada bronze para silver:
    - normaliza nomes
    - aplica clean_data (remove duplicatas, preenche nulos)
    - aplica transform_sales_data (cálculos e extração de data)
    - retorna DataFrame silver pronto para agregações
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Padronizar colunas: se existir order_id -> manter, se existir order_date -> renomear para date para as transforms
    if "order_date" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"order_date": "date"})

    # Aplicar limpeza
    df = clean_data(df)

    # Aplicar transformações de vendas (se tiver price/quantity)
    df = transform_sales_data(df)

    # Garantir colunas numéricas/tipos coerentes
    if "total_amount" in df.columns and "total_sales" not in df.columns:
        # alinhar nomenclatura: total_amount -> total_sales
        df["total_sales"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0)

    # Ano/mês/dia já devem existir a partir de transform_sales_data
    return df


def silver_to_gold(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Gera as tabelas Gold a partir do silver:
    - monthly_sales: receita por ano/mês (DataFrame com columns: year, month, revenue, orders_count)
    - product_performance: soma total por produto (DataFrame)
    - regional_sales: soma total por região (DataFrame) (se coluna region existir)
    Retorna dicionário com DataFrames.
    """
    if df is None or df.empty:
        return {
            "monthly_sales": pd.DataFrame(columns=["year", "month", "revenue", "orders_count"]),
            "product_performance": pd.DataFrame(columns=["product_name", "total_sales"]),
            "regional_sales": pd.DataFrame(columns=["region", "total_sales"])
        }

    df = df.copy()

    # Certificar que total_sales existe
    if "total_sales" not in df.columns:
        if "total_amount" in df.columns:
            df["total_sales"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0)
        else:
            df["total_sales"] = 0.0

    # Monthly revenue
    if "year" in df.columns and "month" in df.columns:
        monthly = (
            df.groupby(["year", "month"], dropna=False)["total_sales"]
            .agg(revenue="sum", orders_count="count")
            .reset_index()
        )
    else:
        # tentar extrair a partir da coluna date
        if "date" in df.columns:
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            monthly = (
                df.groupby(["year", "month"], dropna=False)["total_sales"]
                .agg(revenue="sum", orders_count="count")
                .reset_index()
            )
        else:
            monthly = pd.DataFrame(columns=["year", "month", "revenue", "orders_count"])

    # Product performance
    if "product_name" in df.columns:
        product_perf = (
            df.groupby("product_name", dropna=False)["total_sales"]
            .sum()
            .reset_index()
            .rename(columns={"total_sales": "total_sales"})
        )
    else:
        product_perf = pd.DataFrame(columns=["product_name", "total_sales"])

    # Regional sales
    if "region" in df.columns:
        regional = (
            df.groupby("region", dropna=False)["total_sales"]
            .sum()
            .reset_index()
            .rename(columns={"total_sales": "total_sales"})
        )
    else:
        regional = pd.DataFrame(columns=["region", "total_sales"])

    return {
        "monthly_sales": monthly,
        "product_performance": product_perf,
        "regional_sales": regional
    }


# Exports
__all__ = [
    "clean_data",
    "transform_sales_data",
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
]
