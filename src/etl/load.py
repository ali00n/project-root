from sqlalchemy import create_engine, text
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_postgres(df, table_name, connection_string, schema='public', if_exists='replace'):
    """Carrega DataFrame para PostgreSQL"""
    try:
        logger.info(f"Carregando dados na tabela {schema}.{table_name}")
        engine = create_engine(connection_string)

        # Carrega os dados
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi'
        )

        logger.info(f"Dados carregados com sucesso: {schema}.{table_name} ({len(df)} linhas)")
        return True

    except Exception as e:
        logger.error(f"Erro ao carregar dados na tabela {table_name}: {e}")
        return False


def load_to_csv(df, file_path, index=False):
    """Salva DataFrame como CSV"""
    try:
        logger.info(f"Salvando dados em CSV: {file_path}")

        # Cria diretório se não existir
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        df.to_csv(file_path, index=index)
        logger.info(f"Arquivo CSV salvo: {file_path}")
        return True

    except Exception as e:
        logger.error(f"Erro ao salvar CSV {file_path}: {e}")
        return False


def execute_sql_file(file_path, connection_string):
    """Executa arquivo SQL no PostgreSQL"""
    try:
        logger.info(f"Executando arquivo SQL: {file_path}")

        if not os.path.exists(file_path):
            logger.error(f"Arquivo SQL não encontrado: {file_path}")
            return False

        with open(file_path, 'r', encoding='utf-8') as file:
            sql_commands = file.read()

        engine = create_engine(connection_string)
        with engine.connect() as conn:
            # Divide os comandos SQL por ponto e vírgula
            commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]

            for command in commands:
                if command:  # Evita comandos vazios
                    try:
                        conn.execute(text(command))
                        logger.debug(f"Comando executado: {command[:50]}...")
                    except Exception as e:
                        logger.warning(f"Erro ao executar comando SQL: {e}")
                        continue

            conn.commit()

        logger.info(f"Arquivo SQL executado com sucesso: {file_path}")
        return True

    except Exception as e:
        logger.error(f"Erro ao executar arquivo SQL {file_path}: {e}")
        return False


def create_data_lake_structure(connection_string):
    """Cria a estrutura do data lake no PostgreSQL"""
    sql_commands = """
    -- Criar schemas para camadas do Data Lake
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;

    -- Comentários nos schemas
    COMMENT ON SCHEMA raw IS 'Camada Raw: Dados brutos sem tratamento';
    COMMENT ON SCHEMA bronze IS 'Camada Bronze: Dados brutos com estrutura definida';
    COMMENT ON SCHEMA silver IS 'Camada Silver: Dados limpos e enriquecidos';
    COMMENT ON SCHEMA gold IS 'Camada Gold: Dados agregados para business intelligence';
    """

    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            conn.execute(text(sql_commands))
            conn.commit()
        logger.info("Estrutura do Data Lake criada com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar estrutura do Data Lake: {e}")
        return False