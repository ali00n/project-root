from src.etl.extract import extract_from_csv, generate_sample_data
from src.etl.transform import bronze_to_silver, silver_to_gold
from src.etl.load import (
    load_to_postgres,
    execute_sql_file,
    create_data_lake_structure,
    load_to_csv
)
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Pipeline ETL completo"""

    # Configurações
    DB_CONNECTION = "postgresql://pguser:pgpass@localhost:5432/dd_project"
    DATA_FILE = "datasets/sample_data.csv"

    try: # PRINT PARA SER MELHOR VISUALIAZAO NO TERMINAL
        print("=" * 50)
        print("INICIANDO PIPELINE ETL COMPLETO")
        print("=" * 50)

        # Criar estrutura do Data Lake
        print(" Criando estrutura do Data Lake...")
        create_data_lake_structure(DB_CONNECTION)

        # Gerar dados de exemplo se não existir
        if not os.path.exists(DATA_FILE):
            print(" Gerando dados de exemplo...")
            sample_data = generate_sample_data()
            load_to_csv(sample_data, DATA_FILE)
            raw_data = sample_data
        else:
            print(" Extraindo dados do arquivo CSV...")
            raw_data = extract_from_csv(DATA_FILE)

        # EXTRACT - Camada Raw
        print(f" Dados extraídos: {len(raw_data)} linhas")

        # TRANSFORM - Raw to Bronze (dados brutos estruturados)
        print(" Transformando Raw → Bronze...")
        bronze_data = raw_data.copy()  # Placeholder

        # TRANSFORM - Bronze to Silver
        print(" Transformando Bronze → Silver...")
        silver_data = bronze_to_silver(bronze_data)

        # TRANSFORM - Silver → Gold
        print(" Transformando Silver → Gold...")
        gold_data_dict = silver_to_gold(silver_data)

        # LOAD - Para PostgreSQL
        print(" Carregando dados nas camadas...")

        # Bronze
        load_to_postgres(bronze_data, "sales_bronze", DB_CONNECTION, schema="bronze")

        # Silver
        load_to_postgres(silver_data, "sales_silver", DB_CONNECTION, schema="silver")

        # Gold (três tabelas)
        for table_name, gold_df in gold_data_dict.items():
            load_to_postgres(gold_df, table_name, DB_CONNECTION, schema="gold")

        print(" Executando transformações SQL...")

        # EXECUÇÃO DE SCRIPTS SQL
        sql_files = [
            "src/sql/00_create_schemas_and_tables.sql",
            "src/sql/01_create_indexes.sql",
            "src/sql/02_business_views.sql"
        ]

        # iterando sobre o sql_files(arquivos do sql)
        for sql_file in sql_files:
            if os.path.exists(sql_file):
                execute_sql_file(sql_file, DB_CONNECTION)

        print("=" * 50)
        print("PIPELINE ETL CONCLUÍDO COM SUCESSO!")
        print("=" * 50)

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
