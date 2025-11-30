-- Script de criação de schemas e tabelas
-- Este script é executado automaticamente pelo pipeline ETL < -----

-- Schemas já foram criados pelo python mas vamos garantimos aqui também
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Comentários para documentaao
COMMENT ON SCHEMA raw IS 'Dados brutos diretamente da fonte';
COMMENT ON SCHEMA bronze IS 'Dados validados mas ainda brutos';
COMMENT ON SCHEMA silver IS 'Dados limpos e enriquecidos para análise';
COMMENT ON SCHEMA gold IS 'Dados agregados e métricas de negócio';
