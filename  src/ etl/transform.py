import pytest
import pandas as pd
import sys
import os

# Adiciona o src ao path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from transform import clean_data, transform_sales_data, bronze_to_silver, silver_to_gold


class TestTransform:

    def test_clean_data(self):
        """Testa a função de limpeza de dados"""
        # Cria dados de teste com problemas
        test_data = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Alice', None, 'Charlie'],
            'age': [25, 30, 25, 35, None],
            'salary': [50000, 60000, 50000, 70000, 80000],
            'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03', '2024-01-04']
        })

        cleaned = clean_data(test_data)

        # Verifica se removeu duplicatas
        assert len(cleaned) == 4  # Uma duplicata removida

        # Verifica se preencheu valores nulos
        assert cleaned['name'].isnull().sum() == 0
        assert cleaned['age'].isnull().sum() == 0

        # Verifica se converteu datas
        assert pd.api.types.is_datetime64_any_dtype(cleaned['date'])

    def test_transform_sales_data(self):
        """Testa transformações específicas de vendas"""
        test_data = pd.DataFrame({
            'product_name': ['A', 'B', 'C'],
            'price': [10.0, 20.0, 30.0],
            'quantity': [2, 3, 1],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
        })

        transformed = transform_sales_data(test_data)

        # Verifica se criou coluna total_sales
        assert 'total_sales' in transformed.columns
        assert transformed['total_sales'].tolist() == [20.0, 60.0, 30.0]

        # Verifica se extraiu componentes de data
        assert 'year' in transformed.columns
        assert 'month' in transformed.columns
        assert 'day' in transformed.columns

    def test_bronze_to_silver(self):
        """Testa transformação bronze para silver"""
        test_data = pd.DataFrame({
            'transaction_id': [1, 2, 3, 4, 5],
            'product_name': ['A', 'B', 'A', 'C', None],
            'price': [10.0, 20.0, 10.0, 30.0, 40.0],
            'quantity': [1, 2, 1, 3, 1],
            'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03', '2024-01-04']
        })

        silver_data = bronze_to_silver(test_data)

        # Verificações básicas
        assert len(silver_data) <= len(test_data)  # Pode ter removido duplicatas/nulos
        assert 'total_sales' in silver_data.columns
        assert 'year' in silver_data.columns

    def test_silver_to_gold(self):
        """Testa transformação silver para gold"""
        test_data = pd.DataFrame({
            'product_name': ['A', 'B', 'A', 'C', 'B'],
            'price': [10.0, 20.0, 10.0, 30.0, 20.0],
            'quantity': [1, 2, 1, 3, 1],
            'total_sales': [10.0, 40.0, 10.0, 90.0, 20.0],
            'year': [2024, 2024, 2024, 2024, 2024],
            'month': [1, 1, 2, 2, 2],
            'region': ['North', 'South', 'North', 'East', 'West']
        })

        gold_data = silver_to_gold(test_data)

        # Verifica se retornou um dicionário com as tabelas gold
        assert isinstance(gold_data, dict)
        assert 'monthly_sales' in gold_data
        assert 'product_performance' in gold_data
        assert 'regional_sales' in gold_data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])