# Projeto de Big Data — Tabela FIPE

## 🔍 Descrição do Problema
- Necessidade de acessar valores válidos de veículos pela Tabela FIPE.  
- Dificuldade para extrair dados sem ser bloqueado pela API.  

## ✅ Solução Proposta
- Consumir dados válidos da FIPE via API e exibir todas as informações.  
- Facilitar o acesso seguro aos dados.  
- Utilizar informações atualizadas da API em tempo real.  

## 📌 Escopo do Projeto
O projeto inclui:
- Coleta dos dados.  
- Processamento dos dados coletados.  
- Armazenamento dos dados processados.  
- Análise exploratória e estatística dos dados.  

## 🏗 Arquitetura Geral
```
API FIPE → Python (coleta e processamento)
        → Jupyter Notebook (testes e validações)
        → CSV/Parquet (armazenamento)
        → Visualizações
```

## 🛠 Tecnologias Utilizadas
- **Linguagem:** Python 3.12  
- **Ferramentas:** Jupyter Notebook, GitHub  
- **Armazenamento:** CSV, Parquet  
- **Bibliotecas:** seaborn, matplotlib, pandas, sqlalchemy, psycopg2-binary, requests, python-dotenv, pytest, jupyter, openpyxl  

## 🔄 Ingestão de Dados
- Uso de requests para coleta dos dados e filtrar para preços de modelos 
entre 18 e 30 mil Reais, final convertendo apenas 5 modelos mais caros

## 🧹 Processamento
- Limpeza de dados.  
- Normalização de tipos.  
- Combinações marca → modelo → ano.  

## 🗃 Armazenamento
- Estrutura em camadas (raw, processed).  
- Arquivos CSV e Parquet.  

## 📊 Análises Realizadas
- Valores brutos retornados da API 
- ⁠Schema bronze para os brutos
- ⁠Schema Silver para tratamento de apenas 10 modelos com duas marcas (YAMAHA, HONDA) 
  apenas pegando preços de motos entre 18k e 30k
- ⁠Schema gold para pegar 5 maiores precos do Schema silver  
- ⁠após todos schemas atualizara no gráfico do matplotlib

## ⚠ Limitações do Projeto
- API mesmo com requisições limitadas a 300 por minuto, se for muito rápido ela cai

## 🚀 Melhorias Futuras
- Dashboards avançados com histórico de valores.  
- Criação de um modelo preditivo.  

## 👤 Papel Individual no Projeto
- Ingestão, limpeza, análise dos dados e desenvolvimento Python.

---

# 🚧 Como executar o projeto (será finalizado depois)  
> Esta seção será ajustada após os testes.  

---

# 🧰 Instruções Técnicas (Docker, Banco e MinIO)

## 1. Navegar para a pasta do projeto:
```
cd C:\\Users\\aliss\\Projetos_faculdade\\project-root\\infra
```

## 2. Iniciar todos os serviços:
```
docker-compose up -d
```

### Verificar status dos containers:
```
docker-compose ps
```

### Ver logs:
#### PostgreSQL
```
docker-compose logs postgres
```

#### MinIO
```
docker-compose logs minio
```

#### Logs em tempo real
```
docker-compose logs -f postgres
```

---

## 3. Testar as conexões
Aguardar 30 segundos:
```
timeout 30
```

### Testar PostgreSQL
```
docker-compose exec postgres pg_isready -U postgres
```

### Conectar ao banco:
```
docker-compose exec postgres psql -U postgres -d fipe_banco
```

#### Comandos dentro do PostgreSQL:
```
\l
\dn
\q
```

### Testar MinIO
Acessar:
```
http://localhost:9001
Usuário: minioadmin
Senha: minioadmin
```

Via CLI:
```
docker-compose exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker-compose exec minio mc ls myminio
```

---

## 5. Verificar dados inseridos
```
cd infra
docker-compose exec postgres psql -U postgres -d fipe_banco -c "SELECT * FROM bronze.fipe_raw LIMIT 5;"
docker-compose exec postgres psql -U postgres -d fipe_banco -c "SELECT * FROM silver.fipe_limited LIMIT 5;"
docker-compose exec postgres psql -U postgres -d fipe_banco -c "SELECT * FROM gold.fipe_summary LIMIT 5;"
```
