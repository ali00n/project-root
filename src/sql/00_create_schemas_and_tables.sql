-- =====================================================
-- SCHEMAS
-- =====================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =====================================================
-- BRONZE — DADOS CRUDOS DA API FIPE
-- (EXATAMENTE COMO O PYTHON INSERE)
-- =====================================================
CREATE TABLE IF NOT EXISTS bronze.fipe_raw (
    id SERIAL PRIMARY KEY,

    marca VARCHAR(50) NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    ano_modelo VARCHAR(10) NOT NULL,

    codigo_marca INTEGER NOT NULL,
    codigo_modelo INTEGER NOT NULL,
    codigo_ano VARCHAR(20) NOT NULL,

    valor VARCHAR(30),
    valor_numeric NUMERIC(12,2)
);

CREATE INDEX IF NOT EXISTS idx_bronze_marca
    ON bronze.fipe_raw(marca);

CREATE INDEX IF NOT EXISTS idx_bronze_modelo
    ON bronze.fipe_raw(modelo);

CREATE INDEX IF NOT EXISTS idx_bronze_valor
    ON bronze.fipe_raw(valor_numeric);

-- =====================================================
-- SILVER — DADOS FILTRADOS (18k–30k)
-- =====================================================
CREATE TABLE IF NOT EXISTS silver.fipe_limited (
    id SERIAL PRIMARY KEY,

    marca VARCHAR(50) NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    ano_modelo VARCHAR(10) NOT NULL,

    valor_numeric NUMERIC(12,2)
);

CREATE INDEX IF NOT EXISTS idx_silver_valor
    ON silver.fipe_limited(valor_numeric);

-- =====================================================
-- GOLD — VISÃO ANALÍTICA (MÉDIAS POR MODELO)
-- =====================================================
CREATE TABLE IF NOT EXISTS gold.fipe_summary (
    id SERIAL PRIMARY KEY,

    marca VARCHAR(50) NOT NULL,
    modelo VARCHAR(100) NOT NULL,

    media_valor NUMERIC(12,2),
    qtd_registros INTEGER
);

CREATE INDEX IF NOT EXISTS idx_gold_media
    ON gold.fipe_summary(media_valor);

-- =====================================================
-- ANALYZE (OTIMIZAÇÃO)
-- =====================================================
ANALYZE bronze.fipe_raw;
ANALYZE silver.fipe_limited;
ANALYZE gold.fipe_summary;
