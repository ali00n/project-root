 # 1 Navegar para a pasta do projeto:
cd C:\Users\aliss\Projetos_faculdade\project-root\infra

# 2 Iniciar todos os serviços:
docker-compose up -d

# Verificar status dos containers:
docker-compose ps

# Verificar logs (opcional):
Ver logs do PostgreSQL
docker-compose logs postgres

Ver logs do MinIO
docker-compose logs minio

Ver logs em tempo real
docker-compose logs -f postgres

# PASSO 3: TESTAR AS CONEXÕES

 Aguardar 30 segundos para inicialização completa
timeout 30

 Testar se PostgreSQL está pronto
docker-compose exec postgres pg_isready -U postgres

 Conectar ao banco de dados
docker-compose exec postgres psql -U postgres -d fipe_banco

# Comandos dentro do PostgreSQL:
-- Listar bancos de dados
\l

-- Listar schemas
\dn

-- Sair do PostgreSQL
\q

# Testar conexão com MinIO:
 Acessar via navegador:
 http://localhost:9001
 Usuário: minioadmin
 Senha: minioadmin

 Ou via linha de comando:
docker-compose exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker-compose exec minio mc ls myminio

# PASSO 5: VERIFICAR OS DADOS INSERIDOS
Verificar dados no PostgreSQL:
cd infra
docker-compose exec postgres psql -U postgres -d fipe_banco -c "SELECT * FROM bronze.fipe_raw LIMIT 5;"
docker-compose exec postgres psql -U postgres -d fipe_banco -c "SELECT * FROM silver.fipe_limited LIMIT 5;"
docker-compose exec postgres psql -U postgres -d fipe_banco -c "SELECT * FROM gold.fipe_summary LIMIT 5;"


