# 1. Iniciar containers
docker-compose up -d

# 2. Verificar se estão rodando
docker-compose ps

# 3. Conectar ao PostgreSQL
docker exec -it postgres_fipe psql -U postgres -d fipe_banco

# 4. No psql, verificar se o banco foi criado
\l  # Listar bancos
\dt *.*  # Listar todas as tabelas

# 5. Acessar MinIO
# Navegador: http://localhost:9001
# Usuário: minioadmin
# Senha: minioadmin