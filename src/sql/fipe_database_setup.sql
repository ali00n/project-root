-- ================================
--     BANCO FIPE  (POSTGRESQL)
-- ================================

-- 1) Criar o banco
CREATE DATABASE fipe_banco;

-- =====================================
-- 2) Criar SCHEMAS dentro do banco
-- =====================================
-- Rode estes comandos APÓS conectar ao banco fipe_banco:

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =====================================
-- 3) Criar tabelas do projeto FIPE
-- =====================================

-- ================
-- BRONZE (RAW)
-- ================
CREATE TABLE IF NOT EXISTS bronze.fipe_raw (
    id SERIAL PRIMARY KEY,
    marca VARCHAR(100),
    modelo VARCHAR(200),
    ano_modelo VARCHAR(20),
    codigo_marca VARCHAR(20),
    codigo_modelo VARCHAR(20),
    codigo_ano VARCHAR(20),
    valor VARCHAR(50),
    valor_numeric NUMERIC(12,2),
    timestamp TIMESTAMP DEFAULT NOW()
);

-- ================
-- SILVER (refinada)
-- Apenas motos com valor entre 18k e 30k
-- ================
CREATE TABLE IF NOT EXISTS silver.fipe_limited (
    id SERIAL PRIMARY KEY,
    marca VARCHAR(100),
    modelo VARCHAR(200),
    ano_modelo VARCHAR(20),
    valor_numeric NUMERIC(12,2),
    timestamp TIMESTAMP DEFAULT NOW()
);

-- ================
-- GOLD (agregações)
-- Média por marca-modelo
-- ================
CREATE TABLE IF NOT EXISTS gold.fipe_summary (
    id SERIAL PRIMARY KEY,
    marca VARCHAR(100),
    modelo VARCHAR(200),
    media_valor NUMERIC(12,2),
    qtd_registros INT,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- =====================================
-- 4) Criar índices para melhorar performance
-- =====================================

-- BRONZE
CREATE INDEX IF NOT EXISTS idx_fipe_raw_marca ON bronze.fipe_raw(marca);
CREATE INDEX IF NOT EXISTS idx_fipe_raw_valor ON bronze.fipe_raw(valor_numeric);

-- SILVER
CREATE INDEX IF NOT EXISTS idx_fipe_limited_valor ON silver.fipe_limited(valor_numeric);

-- GOLD
CREATE INDEX IF NOT EXISTS idx_fipe_summary_marca ON gold.fipe_summary(marca);

-- =====================================
-- 5) CONSULTAS PRONTAS
-- =====================================

-- BRONZE: tudo coletado
-- SELECT * FROM bronze.fipe_raw ORDER BY valor_numeric DESC;

-- SILVER: somente entre R$ 18.000 e R$ 30.000
-- SELECT * FROM silver.fipe_limited ORDER BY valor_numeric DESC;

-- GOLD: médias por modelo
-- SELECT * FROM gold.fipe_summary ORDER BY media_valor DESC;

