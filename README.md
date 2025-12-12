# 📊 Projeto FIPE – Ingestão de Dados com Python, PostgreSQL e MinIO

Este projeto realiza a ingestão automática de dados da **API da Tabela FIPE**, armazenando as informações em um **PostgreSQL** e, quando necessário, em um **MinIO (S3 local)**.  
A infraestrutura é totalmente gerenciada via **Docker Compose**.

---

## 🚀 Tecnologias Utilizadas

- Python 3.12+
- Docker e Docker Compose
- PostgreSQL 15
- MinIO (S3 compatível)
- API pública da FIPE

---

## 📁 Estrutura do Projeto

```text
project-root/
│
├── infra/
│   └── docker-compose.yml
│
├── src/
│   ├── insert_api_automacao.py
│   ├── services/
│   ├── sql/
│   │   ├── 00_create_schemas_and_tables.sql
│   │   ├── 01_raw_ingest.sql
│   │   ├── 02_business_views.sql
│   │   └── fipe_database_setup.sql
│   └── __init__.py
│
├── .venv/
└── README.md

🐳 Subindo a Infraestrutura (Postgres + MinIO)

Entre na pasta infra:
cd infra

Suba os containers:
docker-compose up -d

Verifique se estão rodando:
docker-compose ps

Você deve ver:
postgres_fipe
minio_fipe

🗄️ Configuração do Banco de Dados
Dados do PostgreSQL

Host: localhost

Porta: 5432

Usuário: postgres

Senha: postgres

Banco: fipe_banco

Executar os scripts SQL:
docker-compose exec postgres psql -U postgres -d fipe_banco -f /app/src/sql/00_create_schemas_and_tables.sql
docker-compose exec postgres psql -U postgres -d fipe_banco -f /app/src/sql/01_raw_ingest.sql
docker-compose exec postgres psql -U postgres -d fipe_banco -f /app/src/sql/02_business_views.sql


🪣 Acessando o MinIO

URL Console: http://localhost:9001

Usuário: minioadmin

Senha: minioadmin

API: http://localhost:9000


🐍 Ambiente Python

Crie e ative o ambiente virtual:
python -m venv .venv
.venv\Scripts\activate

Instale as dependências (se houver requirements.txt):
pip install -r requirements.txt


▶️ Executando a Ingestão de Dados

Na raiz do projeto:
python src/insert_api_automacao.py

Esse script:

Consome a API da FIPE

Insere dados no PostgreSQL

Pode armazenar dados no MinIO


🔍 Verificando os Dados no Banco:
docker-compose exec postgres psql -U postgres -d fipe_banco

Dentro do psql:
\dn
\dt raw.*;
SELECT COUNT(*) FROM raw.nome_da_tabela;

🛑 Parar os Containers:
docker-compose down

Para remover volumes (⚠ apaga dados):
docker-compose down -v












