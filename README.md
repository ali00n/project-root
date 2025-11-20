# Pipeline ETL - Ciência de Dados

## Como executar

## Inicie os containers:
```bash```
docker-compose -f infra/docker-compose.yml up -d

 ## Instale as dependências:
pip install -r requirements.txt

Execute o pipeline:
python src/etl/run_pipeline.py

## Estrutura
src/etl/: Código do pipeline ETL

src/sql/: Scripts SQL

src/tests/: Testes unitários

infra/: Configuração Docker

notebooks/: Análise exploratória

## 6. Para executar:

```bash
# 1. Iniciar infraestrutura
docker-compose -f infra/docker-compose.yml up -d

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Executar pipeline
python src/etl/run_pipeline.py

