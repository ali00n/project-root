import pandas as pd
import requests
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_from_csv(file_path):
    """Extrai dados de um arquivo CSV"""
    try:
        logger.info(f"Extraindo dados do CSV: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Dados extraídos: {len(df)} linhas, {len(df.columns)} colunas")
        return df
    except Exception as e:
        logger.error(f"Erro ao extrair do CSV {file_path}: {e}")
        raise


def extract_from_api(api_url, params=None):
    """Extrai dados de uma API"""
    try:
        logger.info(f"Extraindo dados da API: {api_url}")
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Se a API retornar uma lista de objetos
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            # Se for um objeto com dados aninhados
            df = pd.json_normalize(data)

        logger.info(f"Dados da API extraídos: {len(df)} linhas")
        return df
    except Exception as e:
        logger.error(f"Erro ao extrair da API {api_url}: {e}")
        raise


def extract_from_postgres(query, connection_string):
    """Extrai dados do PostgreSQL"""
    try:
        logger.info("Extraindo dados do PostgreSQL")
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)
        logger.info(f"Dados do PostgreSQL extraídos: {len(df)} linhas")
        return df
    except Exception as e:
        logger.error(f"Erro ao extrair do PostgreSQL: {e}")
        raise


def generate_sample_data():
    """Gera dados de exemplo para testes"""
    import random
    from datetime import datetime, timedelta

    data = []
    products = ['Product_A', 'Product_B', 'Product_C', 'Product_D', 'Product_E']

    for i in range(1000):
        base_date = datetime(2024, 1, 1)
        random_days = random.randint(0, 365)
        date = base_date + timedelta(days=random_days)

        record = {
            'transaction_id': i + 1,
            'product_name': random.choice(products),
            'quantity': random.randint(1, 10),
            'price': round(random.uniform(10.0, 100.0), 2),
            'date': date.strftime('%Y-%m-%d'),
            'customer_id': random.randint(1000, 1100),
            'region': random.choice(['North', 'South', 'East', 'West'])
        }
        data.append(record)

    df = pd.DataFrame(data)
    return df