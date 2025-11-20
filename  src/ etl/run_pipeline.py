from extract import extract_from_csv, generate_sample_data
from transform import bronze_to_silver, silver_to_gold
from load import load_to_postgres, execute_sql_file, create_data_lake_structure, load_to_csv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Pipeline ETL completo"""

    # Configurações
    DB_CONNECTION = "postgresql://pguser:pgpass@localhost:5432/dd_project"
    DATA_FILE = "datasets/sample_data.csv"

    try:
        print("=" * 50)
        print("INICIANDO PIPELINE ETL COMPLETO")
        print("=" * 50)

        # Criar estrutura do Data Lake
        print("1. Criando estrutura do Data Lake...")
        create_data_lake_structure(DB_CONNECTION)

        # Gerar dados de exemplo se não existir
        if not os.path.exists(DATA_FILE):
            print("2. Gerando dados de exemplo...")
            sample_data = generate_sample_data()
            load_to_csv(sample_data, DATA_FILE)
            raw_data = sample_data
        else:
            print("2. Extraindo dados do arquivo CSV...")
            raw_data = extract_from_csv(DATA_FILE)

        # EXTRACT - Camada Raw
        print(f"3. Dados extraídos: {len(raw_data)} linhas")

        # TRANSFORM - Raw to Bronze (dados brutos estruturados)
        print("4. Transformando Raw → Bronze...")
        bronze_data = raw_data.copy()  # Na prática, aqui teria validações

        # TRANSFORM - Bronze to Silver
        print("5. Transformando Bronze → Silver...")
        silver_data = bronze_to_silver(bronze_data)

        # TRANSFORM - Silver to Gold
        print("6. Transformando Silver → Gold...")
        gold_data_dict = silver_to_gold(silver_data)

        # LOAD - Para PostgreSQL
        print("7. Carregando dados nas camadas...")

        # Camada Bronze
        load_to_postgres(bronze_data, "sales_bronze", DB_CONNECTION, schema="bronze")

        # Camada Silver
        load_to_postgres(silver_data, "sales_silver", DB_CONNECTION, schema="silver")

        # Camada Gold (múltiplas tabelas)
        for table_name, gold_df in gold_data_dict.items():
            load_to_postgres(gold_df, table_name, DB_CONNECTION, schema="gold")

        # Executar transformações SQL adicionais
        print("8. Executando transformações SQL...")
        sql_files = [
            "src/sql/00_create_schemas_and_tables.sql",
            "src/sql/01_create_indexes.sql",
            "src/sql/02_business_views.sql"
        ]

        for sql_file in sql_files:
            if os.path.exists(sql_file):
                execute_sql_file(sql_file, DB_CONNECTION)

        print("=" * 50)
        print("PIPELINE ETL CONCLUÍDO COM SUCESSO!")
        print("=" * 50)

        # Resumo
        print("\nRESUMO DA EXECUÇÃO:")
        print(f"- Bronze: {len(bronze_data)} linhas")
        print(f"- Silver: {len(silver_data)} linhas")
        print(f"- Gold: {sum(len(df) for df in gold_data_dict.values())} linhas totais")

        return True

    except Exception as e:
        logger.error(f"ERRO NO PIPELINE ETL: {e}")
        return False


if __name__ == "__main__":
    run_etl_pipeline()